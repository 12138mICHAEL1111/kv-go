#  KV-GO

A lightweight, fast key/value persistent storage database.

## Key Features

- Sequential disk I/O. Reduce the movement of the disk head，hence the writing speed is fast.

- Index stored in merory, improve reading speed.

- Support batch writing data and ensure the atomicity of the data.

- Support merging(clearing) invalid data.


## Example 
Open :

```go
db, err := Open(DefaultConfig)
db := Open(opts)
```

Write :

```go
_ = db.Put([]byte("hello"),[]byte("world"))
```

Read :

```go
value,_ := db.Get([]byte("hello"))
```

Delete :

```go
_ := db.Delete([]byte("hello"))
```

Merge :

```go
_ := db.Merge()
```

Batch write:
```go
wb := db.NewWriteBatch(DefaultWriteBatchOptions)
err := wb.Put([]byte("hello"),[]byte("world"))
err = wb.Put([]byte("hello2"),[]byte("world2"))
err = wb.Delete([]byte("hello"))
err = wb.Commit()
```

### Cons

- All index must be stored in merory, storage size based on the memory size.
