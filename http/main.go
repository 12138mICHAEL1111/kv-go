package main

import (
	"encoding/json"
	"fmt"
	kv_go "kv-go"
	"log"
	"net/http"
)

var db *kv_go.DB

func init(){
	var err error
	options := kv_go.DefaultConfig
	db,err =kv_go.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db %v", err))
	}
}

func handlePut(writer http.ResponseWriter,request *http.Request){
	var data map[string]string

	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer,err.Error(),http.StatusBadRequest)
		return
	}

	for key,value := range data{
		if err := db.Put([]byte(key),[]byte(value)) ; err != nil {
			http.Error(writer,err.Error(),http.StatusInternalServerError)
			log.Printf("failed to put value in db %v\n", err)
			return
		}
	}
}

func main(){
	http.HandleFunc("/kv_go/put",handlePut)

	_ = http.ListenAndServe("localhost:8080",nil)
}