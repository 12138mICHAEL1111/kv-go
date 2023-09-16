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

var DefaultConfig = Config{
	DirPath:            "/Users/maike/Desktop/kv-database",
	DataFileSize:       256 * 1024 * 1024, // 256MB
	SyncWrites:         false,
	IndexType:          Btree,
}
