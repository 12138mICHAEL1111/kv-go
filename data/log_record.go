package data

import (
	"encoding/binary"
	"errors"
)

type LogRecordType = byte

var (
	ErrInvalidCRC =errors.New("invalid CRC")
)
const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// logrecord header
type logRecordHeader struct{
	crc uint32 
	recordType LogRecordType
	keySize uint32
	valueSize uint32
}
// header长度 crc 4 byte, type 1 byte, keySize 和 valueSize 最长是5byte 
const maxLogRecordHeaderSize =  binary.MaxVarintLen32 * 2 + 4 + 1
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

//解码 拿到header信息
func decodeLogRecordHeader(buf []byte)(*logRecordHeader,int64){
	return nil, 0
}

//校验crc
func getLogRecordCRC(lr *LogRecord,header []byte)uint32{
	return 0
}