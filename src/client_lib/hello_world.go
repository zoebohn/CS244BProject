package main

import (
	"fmt"
	"net"
)

func main() {
	fmt.Println("Hello world")
	conn, err := net.Dial("tcp", ":8000")
	if err != nil {
		fmt.Println("uh oh...")
	}
	fmt.Fprintf(conn, "Hello from Emma and Zoe!")
}
