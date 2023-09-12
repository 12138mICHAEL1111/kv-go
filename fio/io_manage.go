package fio

import "os"

const(
	DataFilePerm = 0644
)

func NewFileIOManager(fileName string) (*FileIO,error){
	// 文件不存在就创建文件
	fd,err := os.OpenFile(fileName, os.O_CREATE | os.O_RDWR | os.O_APPEND,DataFilePerm)
	
	if err != nil{
		return nil,err
	}

	return &FileIO{fd:fd},nil
}
type IOManager interface{
	Read([]byte,int64)(int,error)
	Write([]byte)(int,error)
	Sync() error //持久化数据
	Close() error
	Size()(int64,error)
}