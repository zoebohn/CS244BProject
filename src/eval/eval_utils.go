package eval 

import(
    "locks"
    "strconv"
    "raft"
)

func GenerateLockList(numLocksPerClient int, totalClients int, diffDomains bool) [][]locks.Lock {
    lockList := make([][]locks.Lock, totalClients)
    i := 0
    for i < totalClients {
        lockList[i] = make([]locks.Lock, 0)
        j := 0
        for j < numLocksPerClient {
            var l locks.Lock
            if diffDomains {
                l = locks.Lock("/" + strconv.Itoa(i) + "/" + "lock_" + strconv.Itoa(j))
            } else {
                l =  locks.Lock("lock_" + strconv.Itoa(j) + "," +  strconv.Itoa(i))
            }
            lockList[i] = append(lockList[i], l)
            j++
        }
        i++
    }
    return lockList
}

func GenerateDomainList(totalClients int, diffDomains bool) []locks.Domain {
    domainList := make([]locks.Domain, 0)
    if !diffDomains {
        return domainList
    }
    i := 0
    for i < totalClients {
        d := locks.Domain("/" + strconv.Itoa(i))
        domainList = append(domainList, d)
        i++
    }
    return domainList
}

func GenerateMasterServerList(ipAddr string)[]raft.ServerAddress {
    masterServers := make([]raft.ServerAddress, 0)
    i := 0
    for i < 3 {
        server := raft.ServerAddress(ipAddr + ":" + strconv.Itoa(40000 + i))
        masterServers = append(masterServers, server)
        i++
    }
    return masterServers
}

func GenerateWorkerServerList(ipAddr string)[]raft.ServerAddress {
    workerServers := make([]raft.ServerAddress, 0)
    i := 0
    for i < 3 {
        server := raft.ServerAddress(ipAddr + ":" + strconv.Itoa(40000 + i))
        workerServers = append(workerServers, server)
        i++
    }
    return workerServers
}


