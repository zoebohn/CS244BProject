package main

import (
    "fmt"
    "raft"
    "time"
    "locks"
)

var masterServers = []raft.ServerAddress {"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}

func main() {
    trans, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, nil)
    if err != nil {
        fmt.Println("err: ", err)
        return
    }
    lc, err := locks.CreateLockClient(trans, masterServers)
    if err != nil {
        fmt.Println("error with creating lock client")
        fmt.Println(err)
    } else {
        fmt.Println("successfully created lock client")
    }
/*    fmt.Println("")
    fmt.Println("")
    test_simple(lc)
    fmt.Println("")
    fmt.Println("")
    test_double_acquire(lc)
    fmt.Println("")
    fmt.Println("")
    test_release_unacquired_lock(lc)
    fmt.Println("")
    fmt.Println("")
    test_duplicate_create(lc)
    fmt.Println("")
  */  fmt.Println("")
    test_creating_domains(lc)
    /*fmt.Println("")
    fmt.Println("")
    test_acquire_nonexistant_lock(lc)
    fmt.Println("")
    fmt.Println("")
    *//* Second client */
    /*trans2, err2 := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, nil)
    if err2 != nil {
        fmt.Println("err: ", err)
        return
    }
    lc2, err2 := locks.CreateLockClient(trans2, masterServers)
    if err2 != nil {
        fmt.Println("error with creating lock client")
        fmt.Println(err)
    } else {
        fmt.Println("successfully created lock client")
    }
    fmt.Println("multiple client tests")
    fmt.Println("")
    test_race_domain(lc, lc2)
    fmt.Println("")
    fmt.Println("")
    test_multiple_acquires_2(lc, lc2)
    fmt.Println("")
    fmt.Println("")
    test_release_unacquired_2(lc, lc2)
    fmt.Println("")
    fmt.Println("")
*/
}

/* Create, acquire, and release lock; one client */
func test_simple(lc *locks.LockClient) {
    lock := locks.Lock("simple_lock")
    fmt.Println("create lock")
    create_err := lc.CreateLock(lock)
    if create_err != nil {
        fmt.Println("error with creating")
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created lock")
    }
    fmt.Println("acquire lock")
    id, acquire_err := lc.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        fmt.Println("error with acquiring")
        fmt.Println(acquire_err)
    } else {
        fmt.Println("successfully acquired lock")
    }
    fmt.Println("release lock")
    release_err := lc.ReleaseLock(lock)
    if release_err != nil {
        fmt.Println("error with releasing")
        fmt.Println(release_err)
    } else {
        fmt.Println("successfully released lock")
    } 
}

func test_acquire_nonexistant_lock(lc *locks.LockClient) {
    lock := locks.Lock("doesnotexist")
    fmt.Println("acquire lock")
    id, acquire_err := lc.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        fmt.Println("error with acquiring")
        fmt.Println(acquire_err)
    } else {
        fmt.Println("successfully acquired lock")
    }
}

func test_double_acquire(lc *locks.LockClient) {
    lock := locks.Lock("double_acquire_lock")
    fmt.Println("create lock")
    create_err := lc.CreateLock(lock)
    if create_err != nil {
        fmt.Println("error with creating")
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created lock")
    }
    fmt.Println("acquire lock")
    id, acquire_err := lc.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        fmt.Println("error with acquiring")
        fmt.Println(acquire_err)
    } else {
        fmt.Println("successfully acquired lock")
    }
    fmt.Println("second acquire lock")
    id, acquire_err = lc.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        fmt.Println("error with second acquiring")
        fmt.Println(acquire_err)
    } else {
        fmt.Println("successfully acquired lock twice")
    }
}

func test_release_unacquired_lock(lc *locks.LockClient) {
    lock := locks.Lock("unacquired_lock")
    fmt.Println("create lock")
    create_err := lc.CreateLock(lock)
    if create_err != nil {
        fmt.Println("error with creating")
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created lock")
    }
    fmt.Println("release lock")
    release_err := lc.ReleaseLock(lock)
    if release_err != nil {
        fmt.Println("error with bad releasing")
        fmt.Println(release_err)
    } else {
        fmt.Println("successfully released unacquired lock")
    } 
}

func test_duplicate_create(lc *locks.LockClient) {
    lock := locks.Lock("simple_lock")
    fmt.Println("create lock")
    create_err := lc.CreateLock(lock)
    if create_err != nil {
        fmt.Println("error with creating duplicate")
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created duplicate lock")
    }
}

/* Create domains */
func test_creating_domains(lc *locks.LockClient) {
    domain := locks.Domain("/first")
    fmt.Println("create domain")
    create_err := lc.CreateDomain(domain)
    if create_err != nil {
        fmt.Println("error with creating")
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created domain")
    }

    /* Create dup domain */
    create_err = lc.CreateDomain(domain)
    if create_err != nil {
        fmt.Println("error with second domain creating")
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created duplicate domain")
    }

    /* Create subdomain */
    subdomain := locks.Domain("/first/second")
    create_err = lc.CreateDomain(subdomain)
    if create_err != nil {
        fmt.Println("error with creating subdomain")
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created subdomain")
    }

    /* Create invalid subdomain */
    subdomain = locks.Domain("/first/third/hi")
    create_err = lc.CreateDomain(subdomain)
    if create_err != nil {
        fmt.Println("error with creating bad subdomain")
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created bad subdomain")

    }

    /* Create root domain */
    root_domain := locks.Domain("/")
    create_err = lc.CreateDomain(root_domain)
    if create_err != nil {
        fmt.Println("error with creating root domain")
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created bad root domain")

    }

}

/* Two clients race to create a domain */
func test_race_domain(lc1 *locks.LockClient, lc2 *locks.LockClient) {
    domain := locks.Domain("/first")
    fmt.Println("create domain")
    create_err := lc1.CreateDomain(domain)
    if create_err != nil {
        fmt.Println("error with creating")
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created domain")
    }

    /* Create dup domain */
    create_err = lc2.CreateDomain(domain)
    if create_err != nil {
        fmt.Println("error with second domain creating")
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created duplicate domain")
    }

}

func test_multiple_acquires_2(lc1 *locks.LockClient, lc2 *locks.LockClient) {
    lock := locks.Lock("race_lock")
    fmt.Println("create lock")
    create_err := lc1.CreateLock(lock)
    if create_err != nil {
        fmt.Println("error with creating")
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created lock")
    }
    fmt.Println("acquire lock")
    id, acquire_err := lc1.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        fmt.Println("error with acquiring")
        fmt.Println(acquire_err)
    } else {
        fmt.Println("successfully acquired lock")
    }
    fmt.Println("acquire lock 2")
    id, acquire_err = lc2.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        fmt.Println("error with second acquiring")
        fmt.Println(acquire_err)
    } else {
        fmt.Println("successfully acquired bad lock")
    }

}

func test_release_unacquired_2(lc1 *locks.LockClient, lc2 *locks.LockClient) {
    lock := locks.Lock("race_2_lock")
    fmt.Println("create lock")
    create_err := lc1.CreateLock(lock)
    if create_err != nil {
        fmt.Println("error with creating")
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created lock")
    }
    fmt.Println("acquire lock")
    id, acquire_err := lc1.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        fmt.Println("error with acquiring")
        fmt.Println(acquire_err)
    } else {
        fmt.Println("successfully acquired lock")
    }
    fmt.Println("acquire lock 2")
    release_err := lc2.ReleaseLock(lock)
    if release_err != nil {
        fmt.Println("error with bad release")
        fmt.Println(acquire_err)
    } else {
        fmt.Println("successfully illegally released")
    }

}
