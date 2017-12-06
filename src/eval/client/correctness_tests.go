package main

import (
    "fmt"
    "raft"
    "time"
    "locks"
    "strconv"
)

var masterServers = []raft.ServerAddress {"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}

var numTestsFailed = 0

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
    fmt.Println("")
    fmt.Println("")
    output_test(test_validate(lc), "validate_lock")
    //test rebalancing
    output_test(test_recalcitrant(lc), "recalcitrant_locks")
    output_test(test_rebalancing(lc), "basic_rebalancing")
    output_test(test_rebalancing_domains(lc), "rebalancing domains")
    // test simple operations
    output_test(test_simple(lc), "simple")
    output_test(test_double_acquire(lc), "double_acquire")
    output_test(test_release_unacquired_lock(lc), "release_unacquired")
    output_test(test_duplicate_create(lc), "duplicate_create")
    output_test(test_creating_domains(lc),"create_domain")
    output_test(test_acquire_nonexistant_lock(lc), "nonexistant_lock")
    /* Second client */
    trans2, err2 := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, nil)
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
    output_test(test_race_domain(lc, lc2), "race_domain")
    output_test(test_multiple_acquires_2(lc, lc2), "multiple_acquire")
    output_test(test_release_unacquired_2(lc, lc2), "release_unacquired")
    lc.DestroyLockClient()
    lc2.DestroyLockClient()
    output_test(test_client_fails_and_releases(trans), "client_fails_and_releases")
    fmt.Println("************* NUMBER OF TESTS FAILED: ", numTestsFailed, " *****************")
}

func output_test(success bool, name string) {
    if success {
        fmt.Println("--- ", name, " PASSED ---")
    } else {
        fmt.Println("*** ERROR: ", name, " FAILED ***")
        numTestsFailed++
    }
    fmt.Println("")
}

func test_validate(lc *locks.LockClient) bool {
    lock := locks.Lock("validate_lock")
    create_err := lc.CreateLock(lock)
    if create_err != nil {
       return false 
    }
    id, acquire_err := lc.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
       return false 
    }
    valid, validate_err := lc.ValidateLock(lock, id)
    if !valid || validate_err != nil {
        return false
    }
    valid, validate_err = lc.ValidateLock(lock, id - 1)
    if valid || validate_err != nil {
        return false
    }
    return true
}

func test_rebalancing(lc *locks.LockClient) bool {
    counter := 0
    success := true
    for counter < 5 {
        lock := locks.Lock("simple_lock" + strconv.Itoa(counter))
        create_err := lc.CreateLock(lock)
        if create_err != nil {
            fmt.Println("error with creating " + string(lock))
            fmt.Println(create_err)
            success = false
        }
        counter += 1
    }
    return success
}

func test_recalcitrant(lc *locks.LockClient) bool {
    counter := 0
    success := true
    for counter < 4 {
        lock := locks.Lock("recal_lock" + strconv.Itoa(counter))
        //fmt.Println("create lock")
        create_err := lc.CreateLock(lock)
        if create_err != nil {
            fmt.Println("error with creating " + string(lock))
            fmt.Println(create_err)
            success = false
        } else {
            //fmt.Println("successfully created lock " + string(lock))
        }
        id, acquire_err := lc.AcquireLock(lock)
        if id == -1 || acquire_err != nil {
            fmt.Println("error with acquiring")
            fmt.Println(acquire_err)
            success = false
        } else {
            //fmt.Println("successfully acquired lock")
        }
        counter += 1
    }

    //time.Sleep(5000 * time.Millisecond) 
    counter = 0
    for counter < 4 {
        lock := locks.Lock("recal_lock" + strconv.Itoa(counter))
        release_err := lc.ReleaseLock(lock)
        if release_err != nil {
            fmt.Println("error with releasing")
            fmt.Println(release_err)
            success = false
        } else {
            //fmt.Println("successfully released lock")
        }
        counter += 1
    }

    //time.Sleep(10000 * time.Millisecond)
    counter = 0
    for counter < 4 {
        lock := locks.Lock("recal_lock" + strconv.Itoa(counter))
        id, acquire_err := lc.AcquireLock(lock)
        if id == -1 || acquire_err != nil {
            fmt.Println("error with acquiring")
            fmt.Println(acquire_err)
            success = false
        } else {
            //fmt.Println("successfully acquired lock")
        }
        counter += 1
    }
    return success
}

func test_rebalancing_domains(lc *locks.LockClient) bool {
    counter := 0
    success := true
    err1 := lc.CreateDomain(locks.Domain("/a"))
    if err1 != nil {
        fmt.Println("error with creating domain a")
        fmt.Println(err1)
        success = false
    }
    err2 := lc.CreateDomain(locks.Domain("/b"))
    if err2 != nil {
        fmt.Println("error with creating domain b")
        fmt.Println(err2)
        success = false
    }
    for counter < 2 {
        lock := locks.Lock("/a/lock" + strconv.Itoa(counter))
        create_err := lc.CreateLock(lock)
        if create_err != nil {
            fmt.Println("error with creating " + string(lock))
            fmt.Println(create_err)
            success = false
        }
        counter++
    }
    for counter < 4 {
        lock := locks.Lock("/b/lock" + strconv.Itoa(counter))
        create_err := lc.CreateLock(lock)
        if create_err != nil {
            fmt.Println("error with creating " + string(lock))
            fmt.Println(create_err)
            success = false
        }
        counter++
    }
    counter = 0
    for counter < 2 {
        lock := locks.Lock("/a/lock" + strconv.Itoa(counter))
        id, acquire_err := lc.AcquireLock(lock)
        if id == -1 || acquire_err != nil {
            fmt.Println("error with acquiring")
            fmt.Println(acquire_err)
            success = false
        }
        counter++
    }
    for counter < 4 {
        lock := locks.Lock("/b/lock" + strconv.Itoa(counter))
        id, acquire_err := lc.AcquireLock(lock)
        if id == -1 || acquire_err != nil {
            fmt.Println("error with acquiring")
            fmt.Println(acquire_err)
            success = false
        }
        counter++
    }
    return success

}

/* Create, acquire, and release lock; one client */
func test_simple(lc *locks.LockClient) bool {
    lock := locks.Lock("simple_lock")
    success := true
    create_err := lc.CreateLock(lock)
    if create_err != nil {
        fmt.Println("error with creating")
        fmt.Println(create_err)
        success = false
    }
    id, acquire_err := lc.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        fmt.Println("error with acquiring")
        fmt.Println(acquire_err)
        success = false
    }
    release_err := lc.ReleaseLock(lock)
    if release_err != nil {
        fmt.Println("error with releasing")
        fmt.Println(release_err)
        success = false
    }
    return success
}

func test_acquire_nonexistant_lock(lc *locks.LockClient) bool {
    lock := locks.Lock("doesnotexist")
    success := true
    id, acquire_err := lc.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        fmt.Println(acquire_err)
    } else {
        success = false
    }
    return success
}

func test_double_acquire(lc *locks.LockClient) bool {
    lock := locks.Lock("double_acquire_lock")
    success := true
    create_err := lc.CreateLock(lock)
    if create_err != nil {
        fmt.Println("error with creating")
        fmt.Println(create_err)
        success = false
    }
    id, acquire_err := lc.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        fmt.Println("error with acquiring")
        fmt.Println(acquire_err)
        success = false
    }
    id, acquire_err = lc.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        success = false
    }
    return success
}

func test_release_unacquired_lock(lc *locks.LockClient) bool {
    lock := locks.Lock("unacquired_lock")
    success := true
    create_err := lc.CreateLock(lock)
    if create_err != nil {
        fmt.Println("error with creating")
        fmt.Println(create_err)
        success = false
    }
    release_err := lc.ReleaseLock(lock)
    if release_err != nil {
        fmt.Println(release_err)
    } else {
        success = false 
    }
    return success
}

func test_duplicate_create(lc *locks.LockClient) bool {
    lock := locks.Lock("simple_lock")
    success := true
    create_err := lc.CreateLock(lock)
    if create_err != nil {
        fmt.Println(create_err)
    } else {
        success = false
    }
    return success
}

/* Create domains */
func test_creating_domains(lc *locks.LockClient) bool {
    domain := locks.Domain("/first")
    success := true
    create_err := lc.CreateDomain(domain)
    if create_err != nil {
        fmt.Println(create_err)
        success = false
    }

    /* Create dup domain */
    create_err = lc.CreateDomain(domain)
    if create_err != nil {
        fmt.Println(create_err)
    } else {
        success = false
    }

    /* Create subdomain */
    subdomain := locks.Domain("/first/second")
    create_err = lc.CreateDomain(subdomain)
    if create_err != nil {
        fmt.Println("error with creating subdomain")
        fmt.Println(create_err)
        success = false
    }

    /* Create invalid subdomain */
    subdomain = locks.Domain("/first/third/hi")
    create_err = lc.CreateDomain(subdomain)
    if create_err != nil {
        fmt.Println(create_err)
    } else {
        fmt.Println("successfully created bad subdomain")
        success = false
    }

    /* Create root domain */
    root_domain := locks.Domain("/")
    create_err = lc.CreateDomain(root_domain)
    if create_err != nil {
        fmt.Println("error with creating root domain")
        fmt.Println(create_err)
    } else {
        success = false
        fmt.Println("successfully created bad root domain")
    }
    return success

}

/* Two clients race to create a domain */
func test_race_domain(lc1 *locks.LockClient, lc2 *locks.LockClient) bool {
    domain := locks.Domain("/firsty")
    success := true
    create_err := lc1.CreateDomain(domain)
    if create_err != nil {
        fmt.Println("error with creating")
        fmt.Println(create_err)
        success = false
    }

    /* Create dup domain */
    create_err = lc2.CreateDomain(domain)
    if create_err != nil {
        fmt.Println(create_err)
    } else {
        success = false
        fmt.Println("successfully created duplicate domain")
    }
    return success

}

func test_multiple_acquires_2(lc1 *locks.LockClient, lc2 *locks.LockClient) bool {
    lock := locks.Lock("race_lock")
    success := true
    create_err := lc1.CreateLock(lock)
    if create_err != nil {
        fmt.Println("error with creating")
        fmt.Println(create_err)
        success = false
    }
    id, acquire_err := lc1.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        fmt.Println("error with acquiring")
        fmt.Println(acquire_err)
        success = false
    }
    id, acquire_err = lc2.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        fmt.Println(acquire_err)
    } else {
        success = false
        fmt.Println("successfully acquired bad lock")
    }
    return success

}

func test_release_unacquired_2(lc1 *locks.LockClient, lc2 *locks.LockClient) bool {
    lock := locks.Lock("race_2_lock")
    success := true
    create_err := lc1.CreateLock(lock)
    if create_err != nil {
        fmt.Println("error with creating")
        fmt.Println(create_err)
        success = false
    }
    id, acquire_err := lc1.AcquireLock(lock)
    if id == -1 || acquire_err != nil {
        fmt.Println("error with acquiring")
        fmt.Println(acquire_err)
        success = false
    }
    release_err := lc2.ReleaseLock(lock)
    if release_err != nil {
        fmt.Println(release_err)
    } else {
        success = false
        fmt.Println("successfully illegally released")
    }
    return success

}

func test_client_fails_and_releases(trans *raft.NetworkTransport) bool {
    success := true
    lc, err := locks.CreateLockClient(trans, masterServers)
    if err != nil {
        success = false
        fmt.Println("error with creating lock client")
        fmt.Println(err)
    }
    lock := locks.Lock("client_fail_lock")
    create_err := lc.CreateLock(lock)
    if create_err != nil {
        success = false
        fmt.Println("error with creating")
        fmt.Println(create_err)
    }
    id1, acquire1_err := lc.AcquireLock(lock)
    if id1 == -1 || acquire1_err != nil {
        fmt.Println("error with acquiring")
        fmt.Println(acquire1_err)
        success = false
    }
    destroy_err := lc.DestroyLockClient()
    if destroy_err != nil {
        success = false
        fmt.Println("error with destroying")
        fmt.Println(destroy_err)
    }
    time.Sleep(10*time.Second)
    newlc, err := locks.CreateLockClient(trans, masterServers)
    if err != nil {
        success = false
        fmt.Println("error with creating lock client")
        fmt.Println(err)
    }
    id2, acquire2_err := newlc.AcquireLock(lock)
    if id2 == -1 || acquire2_err != nil {
        fmt.Println("error with acquiring")
        fmt.Println(acquire2_err)
        success = false
    }
    return success
}
