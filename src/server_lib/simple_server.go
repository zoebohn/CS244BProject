package main

import(
	"fmt"
	"raft"
	"io"
	"io/ioutil"
	"time"
	"log"
	"sync"
	"os"
	"net"
	"strconv"
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

	c := makeCluster(3, false, raft.DefaultConfig())
	defer c.Close()

	for {}
	// Start a raft instance(s) using simple FSM (newRaftInstance??)
	// run it on port 8000 and have client connect
	// try to print hello world message.	
}

func makeCluster(n int, bootstrap bool, conf *raft.Config) *cluster {
	if conf == nil {
		conf = raft.DefaultConfig()
	}

	c := &cluster{
		observationCh: make(chan raft.Observation, 1024),
		conf:          conf,
		// Propagation takes a maximum of 2 heartbeat timeouts (time to
		// get a new heartbeat that would cause a commit) plus a bit.
		propagateTimeout: conf.HeartbeatTimeout*2 + conf.CommitTimeout,
		longstopTimeout:  5 * time.Second,
		failedCh:         make(chan struct{}),
	}
	var configuration raft.Configuration

	// Setup the stores and transports
	for i := 0; i < n; i++ {
		dir, err := ioutil.TempDir("", "raft")
		if err != nil {
			fmt.Println("ERROR creating temp dir\n")
		}

		store := raft.NewInmemStore()
		c.dirs = append(c.dirs, dir)
		c.stores = append(c.stores, store)
		c.fsms = append(c.fsms, &simpleFSM{})
		
		snaps, err := raft.NewFileSnapshotStoreWithLogger(dir, 3, nil)
		if err != nil {
			fmt.Println("ERROR creating snapshot store")
		}
		c.snaps = append(c.snaps, snaps)

		addr := &net.TCPAddr{IP: []byte{127,0,0,1}, Port: 8000+i}
		trans, err := raft.NewTCPTransportWithLogger("0.0.0.0:0", addr, 1, 0, nil)
		if err != nil {
			fmt.Println("ERROR with TCP transport")
		}
		c.trans = append(c.trans, trans)
		localID := raft.ServerID(fmt.Sprintf("server-%s", addr))
		if conf.ProtocolVersion < 3 {
			localID = raft.ServerID(strconv.Itoa(i))
		}
		configuration.Servers = append(configuration.Servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       localID,
			Address:  raft.ServerAddress(strconv.Itoa(i)), // PLACEHOLDER
		})
	}

	// TODO: Peers don't know about each other, need to make sure communicating.
	// Wire the transports together
//	c.FullyConnect()

	// Create all the rafts
	c.startTime = time.Now()
	for i := 0; i < n; i++ {
		logs := c.stores[i]
		store := c.stores[i]
		snap := c.snaps[i]
		trans := c.trans[i]

		peerConf := conf
		peerConf.LocalID = configuration.Servers[i].ID

		if bootstrap {
			err := raft.BootstrapCluster(peerConf, logs, store, snap, trans, configuration)
			if err != nil {
				fmt.Println("ERROR bootstrapping cluster")
			}
		}

		raft, err := raft.NewRaft(peerConf, c.fsms[i], logs, store, snap, trans)
		if err != nil {
			fmt.Println("ERROR creating new raft\n")	
		}

		c.rafts = append(c.rafts, raft)
	}
	return c
}

// FullyConnect connects all the transports together.
//func (c *cluster) FullyConnect() {
//	fmt.Printf("[DEBUG] Fully Connecting")
//	for i, t1 := range c.trans {
//		for j, t2 := range c.trans {
//			if i != j {
//				//t1.Connect(t2.LocalAddr(), t2)
//				t2.Connect(t1.LocalAddr(), t1)
//			}
//		}
//	}
//}

type cluster struct {
	dirs             []string
	stores           []*raft.InmemStore
	fsms             []*simpleFSM
	snaps            []*raft.FileSnapshotStore
	trans            []*raft.NetworkTransport
	rafts            []*raft.Raft
	observationCh    chan raft.Observation
	conf             *raft.Config
	propagateTimeout time.Duration
	longstopTimeout  time.Duration
	logger           *log.Logger
	startTime        time.Time

	failedLock sync.Mutex
	failedCh   chan struct{}
	failed     bool
}

func (c *cluster) Close() {
	var futures []raft.Future
	for _, r := range c.rafts {
		futures = append(futures, r.Shutdown())
	}

	// Wait for shutdown
	limit := time.AfterFunc(c.longstopTimeout, func() {
		// We can't FailNowf here, and c.Failf won't do anything if we
		// hang, so panic.
		panic("timed out waiting for shutdown")
	})
	defer limit.Stop()

	for _, f := range futures {
		if err := f.Error(); err != nil {
			fmt.Println("Error with shutdown\n")	
		}
	}

	for _, d := range c.dirs {
		os.RemoveAll(d)
	}
}
