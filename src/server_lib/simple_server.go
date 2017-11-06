package server

import(
	"fmt"
	"raft"
	"io"
)

type simpleFSM struct{
}

type simpleSnapshot struct{
}

func (s *simpleFSM) Apply(log *raft.Log) interface{} {
	fmt.Println(log.Data)
	return nil
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

func main() {
	// Start a raft instance(s) using simple FSM (newRaftInstance??)
	// run it on port 8000 and have client connect
	// try to print hello world message.	
}
