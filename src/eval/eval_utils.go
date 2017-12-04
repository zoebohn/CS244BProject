package eval 

import(
    "locks"
    "strconv"
)

func GenerateLockList(numLocksPerClient int, totalClients int, diffDomains bool) [][]locks.Lock {
    lockList := make([][]locks.Lock, 0)
    i := 0
    j := 0
    for i < totalClients {
        lockList[i] = make([]locks.Lock, 0)
        for j < numLocksPerClient {
            var l locks.Lock
            if diffDomains {
                l = locks.Lock("/" + strconv.Itoa(i) + "/" + "lock_" + strconv.Itoa(j))
            } else {
                l =  locks.Lock("lock_" + strconv.Itoa(i) + "," +  strconv.Itoa(j))
            }
            lockList[i] = append(lockList[i], l)
        }
    }
    return lockList
}
