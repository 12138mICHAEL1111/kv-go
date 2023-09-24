package kv_go

import (
	"bytes"
	"kv-go/index"
)

type Iterator struct{
	indexIter index.Iterator
	db *DB
	config IteratorConfig
}

func (db *DB) NewIterator(config IteratorConfig) *Iterator{
	indexIter := db.index.Iterator(config.Reverse)
	return &Iterator{
		db:db,
		indexIter: indexIter,
		config: config,
	}
}

func (iter *Iterator)Rewind(){
	iter.indexIter.Rewind()
	iter.skipToNext()
}

func (iter *Iterator) Seek(key []byte){
	iter.indexIter.Seek(key)
	iter.skipToNext()
}

func(iter *Iterator) Next() {
	iter.indexIter.Next()
	iter.skipToNext()
}

func (iter *Iterator) Valid() bool{
	return iter.indexIter.Valid()
}

func (iter *Iterator) Key() []byte{
	return iter.indexIter.Key()
}

func (iter *Iterator) Value() ([]byte,error){
	logRecordPos := iter.indexIter.Value()
	iter.db.mu.RLock()
	defer iter.db.mu.RUnlock()
	return iter.db.getValueByPosition(logRecordPos)
}

func(iter *Iterator) Close() {
	iter.indexIter.Close()
}

//过滤prefix, next指针指向符合的数据
func (iter *Iterator) skipToNext(){
	prefixLen := len(iter.config.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; iter.indexIter.Valid();iter.indexIter.Next(){
		key := iter.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(iter.config.Prefix,key[:prefixLen]) == 0 {
			break
		}
	}
}