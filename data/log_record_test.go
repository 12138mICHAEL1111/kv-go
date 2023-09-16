package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord(t *testing.T){
	//正常情况
	rec1 := &LogRecord{
		Key: []byte("name"),
		Value: []byte("hello"),
		Type: LogRecordNormal,
	}

	res1, n1:= EncodeLogRecord(rec1)
	assert.NotNil(t,res1)
	assert.Greater(t,n1,int64(5))
	t.Log(res1)
	t.Log(n1)
}

func TestDecodeLogRecord(t *testing.T){
	rec1 := &LogRecord{
		Key: []byte("name"),
		Value: []byte("hello"),
		Type: LogRecordNormal,
	}
	res1,_:= EncodeLogRecord(rec1)
	logRecordHeader,_:= decodeLogRecordHeader(res1)
	assert.Equal(t,logRecordHeader.keySize,uint32(len(rec1.Key)))
	assert.Equal(t,logRecordHeader.valueSize,uint32(len(rec1.Value)))
	assert.Equal(t,logRecordHeader.recordType,rec1.Type)
}

