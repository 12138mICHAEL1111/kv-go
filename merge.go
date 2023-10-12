package kv_go

import (
	"io"
	"kv-go/data"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName = "-merge"
	mergeFinishedKey = "merge.finished"
)

func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()

	if db.isMerging {
		return ErrMergeInProcess
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	//
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	//将当前活跃文件转换为旧的数据文件
	db.olderFiles[db.activeFile.FileId] = db.activeFile

	//创建新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return nil
	}

	// 记录这个没有参与merge的文件id
	nonMergeFileId := db.activeFile.FileId

	//取出所有需要merge的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}

	// 释放锁，用户会写入新的active file
	db.mu.Unlock()

	// 将merge文件从小到大排序
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	// 如果存在merge文件，删除
	if _, err := os.Stat(mergePath); err != nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// 创建merge文件
	if err := os.Mkdir(mergePath, os.ModePerm); err != nil {
		return err
	}

	mergeConfig := db.config
	mergeConfig.DirPath = mergePath
	mergeConfig.SyncWrites = false
	mergeDB, err := Open(mergeConfig)
	if err != nil {
		return nil
	}

	// 打开hint文件，储存索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	//遍历每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.Read(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			key, _ := parseSeqLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(key)
			// 和内存进行比较
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				// 把batch的key前缀都变成nonTxnSeqNo
				logRecord.Key = createLogRecordKeyWithSeq(key, nonTxnSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return nil
				}
				// 将位置写入hint
				if err := hintFile.WriteHintRecord(key, pos); err != nil {
					return err
				}
			}
			offset = offset + size
		}
	}

	if err := hintFile.Sync(); err != nil {
		return err
	}

	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// merge完成，创建一个代表merge完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}

	mergeFinRecord := &data.LogRecord{
		Key:[]byte(mergeFinishedKey),
		Value : []byte(strconv.Itoa(int(nonMergeFileId))),
	}

	encRecord,_ := data.EncodeLogRecord(mergeFinRecord)

	if err = mergeFinishedFile.Write(encRecord); err != nil {
		return nil
	}

	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.config.DirPath))
	base := path.Base(db.config.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

func (db *DB) loadMergeFiles() error{
	mergePath := db.getMergePath()

	if _,err := os.Stat(mergePath); os.IsNotExist(err){
		return nil
	}

	defer func(){
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries,err := os.ReadDir(mergePath)

	if err != nil {
		return nil
	}

	//查找merge完成的文件是否存在
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries{
		if entry.Name() == data.MergeFinishedFileName{
			mergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	if ! mergeFinished{
		return nil
	}

	//
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	// 删除旧的数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId ++ {
		filePath := data.GetDatafilePath(db.config.DirPath,fileId)
		if _, err := os.Stat(filePath); err == nil {
			if err := os.Remove(filePath); err != nil {
				return err
			}
		}
	}

	//将新的数据文件移动到数据目录中
	for _,fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath,fileName)
		desPath := filepath.Join(db.config.DirPath,fileName)
		if err := os.Rename(srcPath,desPath); err != nil {
			return err
		}
	}

	return nil
}

//获取没有参与merge的datafile id
func (db *DB) getNonMergeFileId(dirPath string) (uint32,error){
	mergeFinishedFile,err := data.OpenMergeFinishedFile(dirPath)
	if err != nil{
		return 0 ,err
	}
	record,_,err := mergeFinishedFile.Read(0)
	if err != nil {
		return 0, err
	}

	nonMergeFileId, err := strconv.Atoi(string(record.Value))

	if err != nil {
		return 0 , err
	}

	return uint32(nonMergeFileId),nil
}

func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.config.DirPath,data.HintFileName)
	if _, err := os.Stat(hintFileName) ; os .IsNotExist(err){
		return nil
	}

	// 打开hint索引文件
	hintFile, err := data.OpenHintFile(db.config.DirPath)
	if err != nil {
		return nil
	}

	//读取索引
	var offset int64 = 0
	for {
		logRecord,size,err := hintFile.Read(offset)
		if err != nil {
			if err == io.EOF{
				break
			}

			return nil
		}
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key,pos)
		offset = offset + size
	}
	return nil
}