package io

const(
	DataFilePerm = 0644
)
type IOManager interface{
	Read([]byte,int64)(int,error)
	Write([]byte)(int,error)
	Sync() error //持久化数据
	Close() error
}