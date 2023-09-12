package fio

import (
	"os"
)

type FileIO struct{
	fd *os.File
}

// 封装磁盘操作接口

func(fio *FileIO) Read(b []byte,offset int64)(int,error){
	return fio.fd.ReadAt(b,offset)
}

func(fio *FileIO) Write(b []byte)(int,error){
	return fio.fd.Write(b)
}

func(fio *FileIO) Sync() error{
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error{
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64,error){
	stat,err := fio.fd.Stat()
	if err != nil {
		return 0, nil 
	}
	return stat.Size(),nil
}
func NewIoManager(fileName string)(IOManager,error){
	return NewFileIOManager(fileName)
}