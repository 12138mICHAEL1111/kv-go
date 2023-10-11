package index

import (
	"bytes"
	"kv-go/data"
	"github.com/google/btree"
)

type Indexer interface{
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) (*data.LogRecordPos,bool)
	Iterator(reverse bool) Iterator
	Size() int
	Close() error
}
// 储存key和数据位置
type Item struct{
	key []byte
	pos *data.LogRecordPos
}

type IndexType = int8

const (
	Btree IndexType = iota + 1

)
func (ai *Item) Less(bi btree.Item) bool{
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}


func NewIndexer(t IndexType) Indexer{
	switch t{
	case Btree:
		return NewBtree()
	default:
		panic("unsupported index type")
	}
}

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 Key 数据
	Key() []byte

	// Value 当前遍历位置的LogRecordPos
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放相应资源
	Close()
}