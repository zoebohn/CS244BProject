package main

import(
    "server_lib"
    "fmt"
	"raft"
	"io"
	"os"
    "os/signal"
)

type simpleFSM struct{
}

type simpleSnapshot struct{
}

func (s *simpleFSM) Apply(log *raft.Log) interface{} {
	fmt.Println("CALLING APPLY")
    fmt.Println(log.Data)
	return 20
}

func (s *simpleFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &simpleSnapshot{}, nil
}

func (s *simpleFSM) Restore(io.ReadCloser) error {
	return nil
}

func (s *simpleSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (s *simpleSnapshot) Release() {
}

func test() {
    server_lib.makeCluster(3, &simpleFSM{})
    c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
