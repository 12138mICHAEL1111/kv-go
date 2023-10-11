package index

import (
	"bytes"
	"kv-go/data"
	"sort"
	"sync"

	"github.com/google/btree"
)


type BTree struct{
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBtree() *BTree{
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put (key []byte, pos *data.LogRecordPos) *data.LogRecordPos{
	it := &Item{key: key,pos:pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos,bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil{
		return nil,false
	}
	return oldItem.(*Item).pos, true
}

func (bt *BTree) Close() error {
	return nil
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBtreeIterator(bt.tree,reverse)
}
type btreeIterator struct{
	currIndex int // 当前遍历位置
	reverse bool // 是否是反向的遍历
	values []*Item //遍历结果
}

func newBtreeIterator(tree *btree.BTree,reverse bool) *btreeIterator{
	var idx int 
	values := make([]*Item,tree.Len())

	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx ++ 
		return true
	}
	if reverse{
		tree.Descend(saveValues)
	}else{
		// 正向遍历，并保存数据到values
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse: reverse,
		values: values,
	}
}

func (iter *btreeIterator) Rewind(){
	iter.currIndex = 0
}

//寻找小于或大于目标的第一个key的位置
func (iter *btreeIterator) Seek(key []byte){
	if iter.reverse{
		iter.currIndex = sort.Search(len(iter.values),func(i int)bool{
			return bytes.Compare(iter.values[i].key,key) <= 0
		})
	}else{
		iter.currIndex = sort.Search(len(iter.values),func(i int)bool{
			return bytes.Compare(iter.values[i].key,key) >=0
		})
	}
}

func (iter *btreeIterator) Next(){
	iter.currIndex += 1
}

func (iter *btreeIterator) Valid() bool{
	return iter.currIndex < len(iter.values)
}

func (iter *btreeIterator) Key()[]byte{
	return iter.values[iter.currIndex].key
}

func (iter *btreeIterator) Value() *data.LogRecordPos{
	return iter.values[iter.currIndex].pos
}

func (iter *btreeIterator) Close(){
	iter.values = nil
}

