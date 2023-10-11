package data

import (
	"fmt"
	"hash/crc32"
	"io"
	"kv-go/fio"
	"path/filepath"
)

const (
	DataFileSuffix string = ".data"
	HintFileName = "hint-index"
	MergeFinishedFileName = "merge-finished"
)

type DataFile struct {
	FileId      uint32
	WriteOffset int64         // 文件写到了哪个位置
	IOManager   fio.IOManager // 就是一个打开文件的实例
}

//打开数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	filePath := GetDatafilePath(dirPath,fileId)
	// 初始化iomanager
	return newDataFile(filePath,fileId)
}

func OpenHintFile(dirPath string)(*DataFile,error){
	filePath := filepath.Join(dirPath,HintFileName)
	return newDataFile(filePath,0)
}

func OpenMergeFinishedFile(dirPath string)(*DataFile,error){
	filePath := filepath.Join(dirPath,MergeFinishedFileName)
	return newDataFile(filePath,0)
}

func GetDatafilePath(dirPath string, fileId uint32) string{
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileSuffix)
}

func newDataFile(filePath string, fileId uint32)(*DataFile,error){
	ioManager, err := fio.NewIoManager(filePath)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:      fileId,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}

func (df *DataFile) Read(offset int64) (*LogRecord, int64, error) {
	//如果最后一条logrecord长度小于maxLogRecordHeaderSize，只需读到文件末尾，防止报eof
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// 读取header信息 header最大长度
	headerbuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, nil
	}

	header, headerSize := decodeLogRecordHeader(headerbuf)

	// 读取到了文件末尾
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出key和value的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{Type: header.recordType}
	if keySize > 0 || valueSize > 0 {
		//从offset+headerSize的位置读取keySize+valueSize长度
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 根据header的其余信息和key value重新计算crc并与储存的crc比较
	// 不一致就代表数据被损坏了
	crc := calcLogRecordCRC(logRecord, headerbuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOffset += int64(n)
	return nil
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	return
}

//写入索引信息到hint文件中
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key: key,
		Value: EncodeLogRecordPos(pos) ,
	}

	logRecord,_ := EncodeLogRecord(record)
	return df.Write(logRecord)
}