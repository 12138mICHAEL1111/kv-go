package data

import (
	
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T){
	datafile1, err := OpenDataFile("/Users/maike/Desktop/kv-database",111)
	assert.Nil(t,err)
	assert.NotNil(t,datafile1)
}

func TestWriteDataFile(t *testing.T){
	datafile1, err := OpenDataFile("/Users/maike/Desktop/kv-database",111)
	assert.Nil(t,err)
	assert.NotNil(t,datafile1)
	err = datafile1.Write([]byte{'c','a'})
	assert.Nil(t,err)
}