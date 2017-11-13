package main

import (
	"fmt"
    "raft"
)

func test() {
	fmt.Println("Hello world")
    var resp raft.ClientResponse
    err := raft.MakeClientRequest(raft.ServerAddress("127.0.0.1:60236"), []byte("hello!"), &resp)
    if err != nil {
        fmt.Println("error %v", err)
    }
    fmt.Println("done")
}
