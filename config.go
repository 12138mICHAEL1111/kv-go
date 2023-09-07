package kv_go

type Config struct{
	DirPath string
	DataFileSize int64
	SyncWrites bool
	IndexType IndexType
}

type IndexType = int8
const (
	Btree IndexType = iota + 1
)