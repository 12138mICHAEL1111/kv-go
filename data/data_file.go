package data

import "kv-go/io"

const (
	DataFileSuffix string = ".data"
)
type DataFile struct{
	FileId uint32
	WriteOffset int64  // 文件写到了哪个位置
	IOManager io.IOManager 
}

//打开数据文件
func OpenDataFile(dirPath string,fileId uint32)(*DataFile, error){
	return nil, nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) Read(offset int64) (*LogRecord,int64,error) {
	return nil,0,nil
}


