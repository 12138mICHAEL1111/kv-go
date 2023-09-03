package kv_go

import (
	"kv-go/data"
	"kv-go/index"
	"sync"
)

// 储存引擎实例
type DB struct{
	config *Config
	mu *sync.RWMutex
	activeFile *data.DataFile
	olderFiles map[uint32]*data.DataFile
	index index.Indexer //内存索引
}

func (db *DB) Put(key []byte,value []byte) error{
	// 判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	log_record := data.LogRecord{
		Key: key,
		Value: value,
		Type: data.LogRecordNormal,
	}
	//写入磁盘
	pos, err := db.appendLogRecord(&log_record)
	if err != nil {
		return err 
	}

	//写入内存
	if ok := db.index.Put(key,pos); !ok{
		return ErrIndexUpdateFailed
	}

	return nil
}

func (db *DB) Get(key []byte)([]byte,error){
	db.mu.RLock()
	defer db.mu.Unlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//先从内存（btree）中取出key对应的信息
	logRecordPos := db.index.Get(key)
	//key不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	//根据文件id找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid{
		dataFile = db.activeFile
	}else{
		dataFile= db.olderFiles[logRecordPos.Fid]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移读取数据
	logRecord,err := dataFile.Read(logRecordPos.Offset)

	if err != nil {
		return nil ,err
	}

	if logRecord.Type == data.LogRecordDeleted{
		return nil,ErrKeyNotFound
	}

	return logRecord.Value,nil
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord)(*data.LogRecordPos,error){
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前活跃文件是否存在
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil{
			return nil,err
		}
	}

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