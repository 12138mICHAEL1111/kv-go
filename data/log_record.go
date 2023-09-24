package data

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

type LogRecordType = byte

var (
	ErrInvalidCRC =errors.New("invalid CRC")
)
const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
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

type TransactionRecord struct{
	Record *LogRecord
	Pos *LogRecordPos
}

//对logrecord编码 返回整条记录编码，长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte,int64){
	// 初始化一个header部分的字节数组
	header := make([]byte,maxLogRecordHeaderSize)

	//第五个字节存放type
	header[4] = logRecord.Type

	// 下一个写入的空余位置
	var index = 5 
	// 对keysize编码，把int转换为byte，返回写入的byte的长度, 具体编码原理https://segmentfault.com/a/1190000020500985
	index = index + binary.PutVarint(header[index:],int64(len(logRecord.Key)))
	index = index + binary.PutVarint(header[index:],int64(len(logRecord.Value)))

	//整条记录的长度
	var size = index + len(logRecord.Key) + len(logRecord.Value)

	encBytes := make([]byte,size)
	// 把header部分拷贝进encBytes，
	copy(encBytes[:index],header[:index])

	copy(encBytes[index:],logRecord.Key)

	copy(encBytes[index+ len(logRecord.Key):],logRecord.Value)

	// 得到crc
	crc := crc32.ChecksumIEEE(encBytes[4:])
	// 把crc转换为4个字节的数组
	binary.LittleEndian.PutUint32(encBytes[:4],crc)

	return  encBytes,int64(size)
}

// 对header解码,返回header，header长度
func decodeLogRecordHeader(buf []byte)(*logRecordHeader,int64){
	if len(buf) <= 4 {
		return nil,0
	}

	header := &logRecordHeader{
		// 把crc从四个字节的数组转换为uint32
		crc : binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	var index = 5 
	// 读取keysize
	keySize,n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index = index + n

	// header
	valueSize,n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index = index + n
	return header, int64(index)
}

//计算crc,residualHeader是去除了crc的header
func calcLogRecordCRC(lr *LogRecord, residualHeader[]byte)uint32{
	if lr == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(residualHeader)
	crc = crc32.Update(crc,crc32.IEEETable,lr.Key)
	crc = crc32.Update(crc,crc32.IEEETable,lr.Value)
	return crc
}