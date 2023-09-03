package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)
// 写入的记录
type LogRecord struct{
	Key []byte
	Value []byte
	Type LogRecordType
}

type LogRecordPos struct{
	Fid uint32 //文件id 哪个文件
	Offset int64 // 文件里位置
}

//对logrecord编码
func EncodeLogRecord(logRecord *LogRecord) ([]byte,int64){
	return  nil, 0
}