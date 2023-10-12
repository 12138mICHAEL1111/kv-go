package kv_go

import (
	"encoding/binary"
	"kv-go/data"
	"sync"
	"sync/atomic"
)

// 流程：
// commit的最后会添加额外一条数据，类型为LogRecordTxnFinished
// 在写入磁盘的key前面添加seqNo,不是批量操作就添加nonTxnSeqNo
// 在初始化db的时候，调用parseSeqLogRecordKey，来查看当前数据是不是批量操作
// 是批量操作就添加到map中
// 读取到类型LogRecordTxnFinished的数据时，就遍历map来添加进内存中
// 如果在批量操作中，出现失败的情况，那么LogRecordTxnFinished这条数据就不会添加，在初始化db时也不会读到，就不会加入内存中
// 这个方法其实会导致储存在磁盘中的key不是真实的key的情况，还需要parse来得到真实的key
// 最好还是在header里加个字段

const nonTxnSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

type WriteBatch struct {
	config        WriteBatchConfig
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord
}

func (db *DB) NewWriteBatch(config WriteBatchConfig) *WriteBatch {
	return &WriteBatch{
		config:        config,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	// 暂存起来
	wb.pendingWrites[string(key)] = logRecord

	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()

	defer wb.mu.Unlock()

	//数据不存在
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		return nil
	}

	// 重复删除
	if wb.pendingWrites[string(key)] != nil {
		return nil
	}

	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}

	wb.pendingWrites[string(key)] = logRecord

	return nil
}

func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if uint(len(wb.pendingWrites)) > wb.config.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 储存位置信息
	positions := make(map[string]*data.LogRecordPos)

	// 遍历pendingWrites
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   createLogRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})

		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	// 写一条表示事务完成的数据
	finishedRecord := &data.LogRecord{
		Key:  createLogRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}

	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 持久化

	if wb.config.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	//更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}

		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.invalidSize += int64(oldPos.Size)
			wb.db.InvalidPiece += 1
		}
	}

	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

func createLogRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	// 把seqNo放在key前面
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq, seqNo)

	encKey := make([]byte, n+len(key))

	copy(encKey[:n], seq[:n])

	copy(encKey[n:], key)
	return encKey
}

// 解析带有seq的key,返回实际的key和seq序列号
func parseSeqLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
