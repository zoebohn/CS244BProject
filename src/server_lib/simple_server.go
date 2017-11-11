package main

import(
	"fmt"
	"raft"
	"io"
	"io/ioutil"
	"time"
	"log"
	"os"
    /*"bufio"
    "strconv"
    "os/signal"*/
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

    CreateNetworkedCluster()
    for {}
}

// This isn't really getting called with ctrl-c so probs not worth it.
func release(envs []*RaftEnv) {
    fmt.Println("releasing connections")
    for _, e := range envs {
        e.Release()
    }
}

func CreateNetworkedCluster() []*RaftEnv{
	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID("first")
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	conf.SnapshotThreshold = 100
	conf.TrailingLogs = 10

	var envs []*RaftEnv

	// Create a single node
	env1 := MakeRaft(conf, true, 0)
	WaitFor(env1, raft.Leader)
    envs = append(envs, env1)

	// Join a few nodes!
    // Can't make client requests and get error when this is commented in!
	/*for i := 0; i < 2; i++ {
		conf.LocalID = raft.ServerID(fmt.Sprintf("next-batch-%d", i))
		env := MakeRaft(conf, false, i+1)
		addr := env.trans.LocalAddr()
		env1.raft.AddVoter(conf.LocalID, addr, 0, 0)
		envs = append(envs, env)
	}*/
    return envs
}

func MakeRaft(conf *raft.Config, bootstrap bool, num int) *RaftEnv {
	// Set the config
	if conf == nil {
		conf = raft.DefaultConfig()
	}

	dir, err := ioutil.TempDir("", "raft")
	if err != nil {
		fmt.Println("err: %v ", err)
	}

	stable := raft.NewInmemStore()

	snap, err := raft.NewFileSnapshotStore(dir, 3, nil)
	if err != nil {
		fmt.Println("err: %v", err)
	}

   /*file, err := os.Create("log_" + strconv.Itoa(num))
   if err != nil {
       fmt.Println("err: %v", err)
   }
   writer := bufio.NewWriter(file)
   */

	env := &RaftEnv{
		conf:     conf,
		dir:      dir,
		store:    stable,
		snapshot: snap,
		fsm:      &simpleFSM{},
		logger:	  log.New(os.Stdout/*writer*/, "", log.Lmicroseconds),
	}
    trans, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, nil)
	if err != nil {
		fmt.Println("err: %v", err)
	}
	env.logger = log.New(os.Stdout/*writer*/, string(trans.LocalAddr())+" :", log.Lmicroseconds)
	env.trans = trans

	if bootstrap {
		var configuration raft.Configuration
		configuration.Servers = append(configuration.Servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       conf.LocalID,
			Address:  trans.LocalAddr(),
		})
		err = raft.BootstrapCluster(conf, stable, stable, snap, trans, configuration)
		if err != nil {
			fmt.Println("err: %v", err)
		}
	}
	log.Printf("[INFO] Starting node at %v", trans.LocalAddr())
	conf.Logger = env.logger
	raft, err := raft.NewRaft(conf, env.fsm, stable, stable, snap, trans)
	if err != nil {
		fmt.Println("err: %v", err)
	}
	env.raft = raft
	return env
}

func WaitFor(env *RaftEnv, state raft.RaftState) error {
	limit := time.Now().Add(200 * time.Millisecond)
	for env.raft.State() != state {
		if time.Now().Before(limit) {
			time.Sleep(10 * time.Millisecond)
		} else {
			return fmt.Errorf("failed to transition to state %v", state)
		}
	}
	return nil
}

type RaftEnv struct {
	dir      string
	conf     *raft.Config
	fsm      *simpleFSM
	store    *raft.InmemStore
	snapshot *raft.FileSnapshotStore
	trans    *raft.NetworkTransport
	raft     *raft.Raft
	logger	 *log.Logger
}

// Release shuts down and cleans up any stored data, its not restartable after this
func (r *RaftEnv) Release() {
	r.Shutdown()
	os.RemoveAll(r.dir)
}

// Shutdown shuts down raft & transport, but keeps track of its data, its restartable
// after a Shutdown() by calling Start()
func (r *RaftEnv) Shutdown() {
	fmt.Println("[WARN] Shutdown node")
	f := r.raft.Shutdown()
	if err := f.Error(); err != nil {
		panic(err)
	}
	r.trans.Close()
}

