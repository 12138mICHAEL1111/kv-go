package kv_go

import (
	"kv-go/utils"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_Merge2(t *testing.T) {
	opts := DefaultConfig
	opts.DataFileSize = 32 * 1024 * 1024
	db, err := Open(opts)
	// defer destroyDB(db)
	assert.Nil(t, err)

	for i := 0; i < 500000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 500000, len(keys))

	// for i := 0; i < 1000000; i++ {
	// 	val, err := db2.Get(utils.GetTestKey(i))
	// 	assert.Nil(t, err)
	// 	assert.NotNil(t, val)
	// }
}

func Test_speed(t* testing.T){
	opts := DefaultConfig
	db, _ := Open(opts)
	db.Close()
}

func Test_read_speed(t *testing.T){
	opts := DefaultConfig
	path := filepath.Join(opts.DirPath,"000000000.data")
	
	b := make([]byte, 32)
	for i := 0 ; i<1000 ; i++{
		file,_:= os.Open(path)
		file.Read(b)
		file.Close()
	}
}

// 有失效的数据，和被重复 Put 的数据
func TestDB_Merge3(t *testing.T) {
	opts := DefaultConfig
	opts.DataFileSize = 32 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}
	for i := 0; i < 10000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 40000; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), []byte("new value in merge"))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 40000, len(keys))

	for i := 0; i < 10000; i++ {
		_, err := db2.Get(utils.GetTestKey(i))
		assert.Equal(t, ErrKeyNotFound, err)
	}
	for i := 40000; i < 50000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, []byte("new value in merge"), val)
	}
}

// merge过程中，有新数据写入
func TestDB_Merge5(t *testing.T) {
	opts := DefaultConfig
	opts.DataFileSize = 32 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50000; i++ {
			err := db.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
		for i := 60000; i < 70000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
			assert.Nil(t, err)
		}
	}()
	err = db.Merge()
	assert.Nil(t, err)
	wg.Wait()

	//重启校验

	defer func() {
		_ = db.Close()
	}()
	assert.Nil(t, err)
	keys := db.ListKeys()
	assert.Equal(t, 10000, len(keys))

	for i := 60000; i < 70000; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}

func Test_close(t *testing.T){
	opts := DefaultConfig
	db, _ := Open(opts)
	db.Close()
	assert.Nil(t,db.index)
}

