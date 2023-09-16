package main

import (
	"encoding/binary"
	"fmt"
)

func main(){
	buf := make([]byte, 10) // 创建一个长度为10的字节切片

	// 编码数字 1 到 buf
	buf[4]=1
	binary.PutVarint(buf[5:],2)

	fmt.Println(buf)

}

