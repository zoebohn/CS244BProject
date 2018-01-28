Start in src directory.

## To run demo:
    1. In first terminal window: go run demo/demo_server.go
    2. In second terminal window: go run demo/demo.go
    Rebalancing factor default setting is 4, can be changed in 
    demo_server.go

## To test correctness:
    1. In first terminal window: go run eval/server/correctness_server.go
    2. In second terminal window: go run eval/client/correctness_tests.go
    Rebalancing factor default setting is 4, can be changed in 
    correctness_server.go.

## To evaluate performance:
# Evaluation overview:
    After setting up the master and worker clusters on different machines,
    the set of locks are created across the different clusters. This is not
    timed. Then the clients all try to acquire and release disjoint lock
    sets simultaneously for 1 minute.

# Evaluation parameters:
    * Rebalancing factor: When a worker cluster holds this many locks, it will
    trigger rebalancing. Make sure you start with enough worker clusters to
    support your rebalancing factor (<rebalancing-factor>).
    * Number of locks per client: The number of locks each client will try to
    acquire and release repeatedly. Choose with your rebalancing factor and
    number of worker clusters (<num-locks-per-client>).
    * Number of total clients: The number of total clients that will be 
    simultaneously try to acquire and release locks (<num-total-clients>).
    * Different domains: Determines if a client's locks should be created in
    a domain specific to the client (locality hint for rebalancing). 0 if
    should all be in the root domain, 1 if each client should get its own
    domain (<diff-domains>)
    * Client number: Used when starting each client individually. Must be a
    unique number 0-n if there are n total clients. Used to make sure that
    there is no contention for locks (<i>).
    
# Run evaluation tests:
    1. Log onto rice machines. You will need 1 for a master cluster, 1
    for each worker cluster, and 1 for each client.
    2. Determine the IP addresses of all machines in the test environment.
    3. In the terminal window for your master cluster: go run eval/server/launch_master.go
    <master-ip-addr> <rebalancing-factor> <worker-1-ip-addr> ... <worker-n-ip-addr>
    4. In the terminal window for your ith worker cluster: go run eval/server/launch_worker.go
    <master-ip-addr> <worker-i-ip-addr>
    5. In the terminal window for any client: go run eval/client/launch_eval_setup.go
    <current-client-ip-addr> <master-ip-addr> <num-locks-per-client> <num-total-clients> <diff-domains>
    6. Start all clients simultaneously. In the terminal window for the ith client: 
    go run eval/client/launch_eval_client.go <client-i-ip-addr> <master-ip-addr> 
    <i> <num-locks-per-client> <num-total-clients> <diff-domains>
    7. The clients will run for 1 minute before printing out stats. You can
    stop them at any time with CTRL-C.
