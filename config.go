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

type IteratorConfig struct{
	Prefix []byte
	Reverse bool
}

var DefaultIteratorConfig = IteratorConfig{
	Prefix: []byte{},
	Reverse: false,
}

type WriteBatchConfig struct{
	MaxBatchNum uint // 一次批量最大数量
	SyncWrites bool
}