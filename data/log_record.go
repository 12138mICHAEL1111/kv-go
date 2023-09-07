package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)
// 写入磁盘数据格式
type LogRecord struct{
	Key []byte
	Value []byte
	Type LogRecordType
}

// 写入索引数据格式
type LogRecordPos struct{
	Fid uint32 //文件id 哪个文件
	Offset int64 // 文件里位置
}

//对logrecord编码
func EncodeLogRecord(logRecord *LogRecord) ([]byte,int64){
	return  nil, 0
}