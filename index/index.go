package index

import (
	"bytes"
	"kv-go/data"

	"github.com/google/btree"
)

type Indexer interface{
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}
// 储存key和数据位置
type Item struct{
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool{
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}