package main

import(
    "locks"
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
    addrs := []raft.ServerAddress{"127.0.0.1:8000","127.0.0.1:8001","127.0.0.1:8002"}
    locks.MakeCluster(3, &simpleFSM{}, addrs)
    c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
