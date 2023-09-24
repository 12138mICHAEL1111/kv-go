package kv_go

import (
	"errors"
	"io"
	"kv-go/data"
	"kv-go/index"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// 储存引擎实例
type DB struct{
	config *Config
	mu *sync.RWMutex
	fileIds []int //用于创建索引
	activeFile *data.DataFile
	olderFiles map[uint32]*data.DataFile
	index index.Indexer //内存索引
	seqNo uint64 // 事务序列号 递增
}

// 开启数据库
func Open(config *Config)(*DB,error){
	//校验配置
	if err := checkConfig(config);err != nil{
		return nil, err
	}

	// 判断数据目录是否存在，不存在就创建
	if _,err := os.Stat(config.DirPath);os.IsNotExist(err){
		if err := os.MkdirAll(config.DirPath,os.ModePerm);err != nil {
			return nil, err
		}
	}

	// 初始化db实例
	db := &DB{
		config: config,
		mu:new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index: index.NewIndexer(config.IndexType),
	}

	//加载数据文件
	if err := db.loadDataFiles(); err!= nil {
		return nil,err 
	}

	if err:= db.initIndex() ; err != nil {
		return nil,err
	}
	return db,nil
}

func (db *DB) Put(key []byte,value []byte) error{
	// 判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	log_record := data.LogRecord{
		Key: createLogRecordKeyWithSeq(key,nonTxnSeqNo),
		Value: value,
		Type: data.LogRecordNormal,
	}
	//写入磁盘
	pos, err := db.appendLogRecordWithLock(&log_record)
	if err != nil {
		return err 
	}

	//写入内存
	if ok := db.index.Put(key,pos); !ok{
		return ErrIndexUpdateFailed
	}

	return nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord)(*data.LogRecordPos,error){
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 写入磁盘
func (db *DB) appendLogRecord(logRecord *data.LogRecord)(*data.LogRecordPos,error){
	// 判断当前活跃文件是否存在
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil{
			return nil,err
		}
	}

	//logrecord加密
	encRecord, size := data.EncodeLogRecord(logRecord)

	//如果写入的数据超出了文件大小,设置当前文件为旧的，打开新的文件
	if db.activeFile.WriteOffset + size > db.config.DataFileSize {
		// 先持久化数据文件，保证已有数据存进磁盘中
		if err := db.activeFile.Sync();err != nil{
			return nil, err 
		}

		db.olderFiles[db.activeFile.FileId] = db.activeFile

		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	offset := db.activeFile.WriteOffset
	//写入磁盘
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 是否持久化
	if db.config.SyncWrites{
		if err := db.activeFile.Sync();err != nil{
			return nil ,err 
		}
	}

	pos := &data.LogRecordPos{Fid: db.activeFile.FileId,Offset: offset}
	return pos,nil
}

// 设置当前活跃文件
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	dataFile,err := data.OpenDataFile(db.config.DirPath,initialFileId)

	if err != nil {
		return err 
	}

	db.activeFile = dataFile
	return nil
}

//查询
func (db *DB) Get(key []byte)([]byte,error){
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//先从内存（btree）中取出key对应的信息
	logRecordPos := db.index.Get(key)
	//key不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByPosition(logRecordPos)
}

// 根据logrecordpos读取数据
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos)([]byte,error){
	//根据文件id找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid{
		dataFile = db.activeFile
	}else{
		dataFile= db.olderFiles[logRecordPos.Fid] //获取旧的文件
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移读取数据
	logRecord,_,err := dataFile.Read(logRecordPos.Offset)

	if err != nil {
		return nil ,err
	}

	if logRecord.Type == data.LogRecordDeleted{
		return nil,ErrKeyNotFound
	}

	return logRecord.Value,nil
}

func checkConfig(config *Config) error{
	if config.DirPath == "" {
		return errors.New("database dir path is empty")
	}

	if config.DataFileSize <= 0 {
		return errors.New("data file size must be greater than 0")
	}

	return nil
}

// 从磁盘中加载数据文件
// 把文件都放进db实例中
func (db *DB) loadDataFiles() error{
	dirEntries,err := os.ReadDir(db.config.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int

	for _, entry := range dirEntries{
		if strings.HasSuffix(entry.Name(), data.DataFileSuffix){
			fileId,err := strconv.Atoi(strings.Split(entry.Name(),".")[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件id排序
	sort.Ints(fileIds)
	db.fileIds = fileIds
	//遍历并打开每个文件(缺点)
	for i, fid := range fileIds {
		dataFile,err := data.OpenDataFile(db.config.DirPath,uint32(fid))
		if err !=  nil {
			return err
		}
		// id最大的就是最新的文件，也是当前活跃文件
		if i == len(fileIds) - 1{
			db.activeFile = dataFile //设置activeFile
		} else{ // 旧数据文件
			db.olderFiles[uint32(fid)] = dataFile // 设置olderFiles
		}
	}
	return nil
}

//创建索引，并设置当前活跃文件offset
func (db *DB) initIndex() error{
	// 空数据库
	if len(db.fileIds) == 0{
		return nil
	}

	updateIndex := func(key []byte,typ data.LogRecordType,pos *data.LogRecordPos){
		var ok bool
		// 删除类型
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		}else{
			// 添加到索引中
			ok = db.index.Put(key,pos)
		}

		if !ok {
			panic("failed to update index at start")
		}
	}

	tracnsactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo uint64 = nonTxnSeqNo

	// 遍历文件id
	for i, fid := range db.fileIds{
		fileId := uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		}else{
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			// 读取logrecord(每一条数据)
			logRecord,size,err := dataFile.Read(offset)
			if err != nil{
				if err == io.EOF { // 数据读完了，正常错误
					break
				}
				return err 
			}

			// 创建索引数据
			logRecordPos := &data.LogRecordPos{
				Fid:fileId,
				Offset: offset,
			}

			// 解析事务key
			key,seqNo := parseSeqLogRecordKey(logRecord.Key)

			// 不是事务提交的
			if seqNo == nonTxnSeqNo{
				updateIndex(key,logRecord.Type,logRecordPos)
			}else{
				// 读取到了事务完成的数据
				if logRecord.Type == data.LogRecordTxnFinished{
					//遍历tracnsactionRecords
					for _, txnRecord := range tracnsactionRecords[seqNo]{
						updateIndex(txnRecord.Record.Key,txnRecord.Record.Type,txnRecord.Pos)
					}

					delete(tracnsactionRecords,seqNo)
				}else{
					logRecord.Key = key
					// 放进tracnsactionRecords中
					tracnsactionRecords[seqNo] = append(tracnsactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos : logRecordPos,
					})
				}
			}

			//更新序列号
			if seqNo > currentSeqNo{
				currentSeqNo = seqNo
			}
			// 更新offset
			offset += size
		}

		//如果是活跃文件，就更新这个文件的writeoff
		if i == len(db.fileIds) - 1{
			db.activeFile.WriteOffset = offset
		}
	}

	db.seqNo = currentSeqNo
	return nil
}

//删除 添加一条logrecord
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先查询key是否存在 key不存在就直接跳过
	if pos := db.index.Get(key);pos == nil {
		return nil
	}

	// 添加logrecord，类型为delete
	logRecord := &data.LogRecord{
		Key: createLogRecordKeyWithSeq(key,nonTxnSeqNo),
		Type: data.LogRecordDeleted,
	}

	_,err := db.appendLogRecordWithLock(logRecord)

	if err != nil {
		return err
	}

	//从内存索引中删除
	ok:= db.index.Delete(key)

	if !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}