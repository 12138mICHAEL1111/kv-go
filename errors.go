package kv_go

import "errors"

var (
	ErrKeyIsEmpty = errors.New("the key is empty")
	ErrIndexUpdateFailed = errors.New("filed to update index")
	ErrKeyNotFound = errors.New("cannot find key")
	ErrDataFileNotFound = errors.New("cannot find datafile")
	ErrDataDirectoryCorrupted = errors.New("database file is corrupted")
	ErrExceedMaxBatchNum = errors.New("exceed max batch num")
	ErrMergeInProcess = errors.New("merge in process")
)