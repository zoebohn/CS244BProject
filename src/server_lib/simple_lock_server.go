package main

import(
	"fmt"
	"raft"
	"io"
	"os"
    "os/signal"
)

type lockFSM struct{
    locked bool
    lockCh chan bool 
    waiter bool
}

type lockSnapshot struct{
}

func (l *lockFSM) Apply(log *raft.Log) interface{} {
    fmt.Println("-->apply")
    if len(log.Data) < 1 {
        return nil
    }
    // Release
    if log.Data[0] == 0 {
        fmt.Println("***release***")
        l.locked = false
        /*if (l.waiter) {
            l.lockedCh <- true
        }*/
    }
    // Acquire
    if log.Data[0] == 1{
        fmt.Println("***acquire***")
        /*if (l.locked) {
            l.waiter = true
            <-l.lockedCh
        }*/
        l.locked = true
    }
	return nil
}

func (l *lockFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &lockSnapshot{}, nil
}

func (l *lockFSM) Restore(io.ReadCloser) error {
	return nil
}

func (l *lockSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (l *lockSnapshot) Release() {
}

func main() {
    makeCluster(3, &lockFSM{})
    c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
