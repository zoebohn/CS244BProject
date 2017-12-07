package main

import (
    "fmt"
    "raft"
    "locks"
    "strconv"
    "bufio"
    "os"
)

var masterServers = []raft.ServerAddress {"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}

var numTestsFailed = 0

func main() {
    trans, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, nil)
    if err != nil {
        fmt.Println("err: ", err)
        return
    }
    fmt.Println("Creating LockClient...")
    lc, err := locks.CreateLockClient(trans, masterServers)
    if err != nil {
        fmt.Println("error with creating lock client")
        fmt.Println(err)
    }
    fmt.Println("")
    fmt.Println("")
    fmt.Println("1. Create Domain")
    fmt.Println("2. Create Lock")
    fmt.Println("3. Acquire Lock")
    fmt.Println("4. Release Lock")
    fmt.Println("")
    fmt.Println("")

    for true {
        reader := bufio.NewReader(os.Stdin)
        switch input, _ := reader.ReadString('\n'); input {
	    case "1":
            fmt.Println("Enter domain: ")
            domain, _ := reader.ReadString('\n')
            create_domain(lc, domain)	
	    case "2":
            fmt.Println("Enter lock: ")
            lock, _ := reader.ReadString('\n')
		    create_lock(lc, lock)
	    case "3":
            fmt.Println("Enter lock: ")
            lock, _ := reader.ReadString('\n')
            acquire_lock(lc, lock)
	    case "4":
            fmt.Println("Enter lock: ")
            lock, _ := reader.ReadString('\n')
            release_lock(lc, lock) 
	    default:
		    fmt.Printf("Invalid input.")
        }
    fmt.Println("")
    fmt.Println("")
    fmt.Println("1. Create Domain")
    fmt.Println("2. Create Lock")
    fmt.Println("3. Acquire Lock")
    fmt.Println("4. Release Lock")
    fmt.Println("")
    fmt.Println("")
    }
}

func create_lock(lc *locks.LockClient, lock string) bool {
    fmt.Println("Creating lock: ", lock)
    lock := locks.Lock(lock)
    create_err := lc.CreateLock(lock)
    if create_err != nil {
        fmt.Println("Error creating lock: ", str(create_err))
        return false
    }
    fmt.Println("Successfully created lock!")
    return true
}

func acquire_lock(lc *locks.LockClient, lock string) bool {
    fmt.Println("Acquiring lock: ", lock)
    lock := locks.Lock("demo_domain/demo_lock")
    id, acquire_err := lc.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
       fmt.Println("Error acquiring lock: ", str(acquire_err))
       return false
    }
    fmt.Println("Successfully acquired lock!")
    return true
}

func create_domain(lc *locks.LockClient, domain string) bool {
    fmt.Println("Creating domain: ", domain)
    success := true
    err1 := lc.CreateDomain(locks.Domain(domain))
    if err1 != nil {
        fmt.Println("Error creating domain: ", str(err1))
        success = false
    }
    fmt.Println("Successfully created domain!")
    return success
}

func release_lock(lc *locks.LockClient, lock string) bool {
    fmt.Println("Releasing lock: ", lock)
    lock := locks.Lock(lock)
    release_err := lc.ReleaseLock(lock)
    if release_err != nil {
        fmt.Println("Error releasing lock: ", str(err1))
        return false
    }
    fmt.Println("Successfully released lock!")
    return true
}
