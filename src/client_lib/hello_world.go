package main

import (
	"fmt"
    "raft"
)

func main() {
	fmt.Println("Hello world")
    var resp raft.ClientResponse
    err := raft.MakeClientRequest("127.0.0.1:59310", nil, 10, &resp)
    if err != nil {
        fmt.Println("error %v", err)
    }
    fmt.Println("done")
}
