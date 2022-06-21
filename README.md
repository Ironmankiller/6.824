# 6.824 Lab

原先的课程代码是基于GOPATH模式来管理依赖的，但是目前go module已经成为了官方推荐的依赖管理方式，所以我对本项目做了一些修改，在src文件夹下使用go mod init ds命令初始化了项目模块，所以调用src下其他文件夹的代码就不需要再使用类似../mr这种模式，而是ds/mr，这样做的好处是便于在windows下使用goland开发（goland默认使用go module管理依赖，GOPATH模式会编译失败），在linux环境远程运行。

## Lab1 MapReduce

- [x] Word Counting Test
- [x] Indexer Test
- [x] Parallel Map Test
- [x] Parallel Reduce Test
- [x] Crash Recover Test
- [ ] Multi-machine MapReduce



**仍需改进的地方：**

1. 本代码只能在一台机器上多进程运行。

2. 错误检测方法与MapReduce论文中采用心跳检测不同，是直接开启一个线程，在其中等待固定时间内是否收到Worker完成任务的回复，这种错误检测存在的问题是，固定的时间不好设置。因为对于不同任务Worker运行的时间不同，如果等待时间过短，有可能任务还没有执行完就被Master置为IDLE并分配给其他Worker，如果等待时间过长，则不能有效检测出真正崩溃的Worker。

3. 由于执行Reduce的Worker和执行Map的Worker可能不是同一台机器，所以Reduce过程中读取中间结果需要采用RPC。

4. 错误恢复机制与论文中的方法不同，在论文所述的实现中，Map任务产生的中间结果是保存在本地磁盘的，如果此时存放该中间结果的Worker崩溃，执行Reduce的Worker就无法从中读取中间结果，所以需要重新将崩溃的Worker上完成的所有Map任务重新置为IDLE，然后再分配给其他Worker。

## Lab2 Raft

- [x] Leader election
- [x] Append log entries
- [x] Safety
- [x] Persist logic
- [x] Reduce the number of rejected
- [x] 日志同步与client提交请求解耦，减少RPC调用次数
- [x] 日志底层数组GC
- [x] 批量apply
- [x] snapshot
- [ ] No-op log

并发测试4000次无FAIL

**仍需改进的地方：**

1. 本代码的持久化基于实验框架提供的Labgob，没有真正利用磁盘持久化。对于磁盘持久化还需要考虑在持久化的过程中磁盘出现问题导致的数据不一致，可能的解决方法是创建临时文件，写成功后再原子修改文件名；另外还需要考虑写入磁盘的代价，如果用户每次请求的都log都落盘，意味着即使是一个简单的读请求也需要几十毫秒的磁盘IO时间，这显然是不能忍受的，所以可能的解决方法是批量地进行持久化，或者采用更高速的持久化介质，比如SSD或者不断电的内存。

2. 论文中Figure8所表达的情形是，新的leader并不知道之前term的log是否已经被提交，所以当且仅当leader自己term的log被复制到大多数它才允许间接apply其他term的日志，换句话说leader拥有至少在自己任期内提交一个log的能力，它才能下决断提交之前的log，因为如果它有这种能力，在它崩溃后，新的leader将从允许它提交的大多数中选择，而从这些服务器中选出来的leader显然不会重复提交任何index上的log。这种解决方案带来的一个问题就是，如果leader长时间无法将自己term的log复制到大多数（可能是网络问题，也可能是根本没有客户请求），那么即使他能够将之前term的log复制到大多数，它也不被允许提交这些log，这就意味着其他term的客户的请求会被阻塞，所以leader迫切地需要知道之前term的log能否提交。一个可能的解决方案是，在新的leader上任后，发送一个command为空的no-op log，通过测试这个log能否被提交来间接将其他term的log提交。

3. preVote策略可以防止被分到少数分区的server不会重复超时选举，导致term递增，所以当其重新与多数分区恢复通信时也不会造成多数分区的leader失效引发重新选举。

4. 可以参考etcd和sofajraft的raft实现，使用更细粒度的batch控制以及pipeline。

## Lab3 Fault-tolerant Key/Value Service

- [x] K/V service based on raft, can deal with unreliable net, restarts, partitions, many clients and keep linearized
- [x] Snapshot for log compaction
- [ ] ReadIndex
- [ ] Lease mechanism

并发测试6000次无FAIL

**实验笔记**

1. 当检测出读请求重复时，不可以用类似写请求的方法直接从哈希表中拿上次的读结果，因为读请求要能够反映出最新的写结果，请求键对应的实际值可能已经被其他写请求修改，所以哈希表中的键值对可能是过期的，解决方法有两种，第一种是走一遍raft流程，第二种是从状态机中查询，这里我用的是第一种。不从哈希表中获取读结果，也就意味着没必要缓存读结果。

2. 必须设置请求超时，因为如果leader在接受客户请求并阻塞等待raft流程返回的结果的过程中失去了leader的身份，并且该请求对应的日志也被新的leader覆盖，那么如果不设置超时，客户会永远等待下去。

3. 这里我为raft每个index上的log都创建了一个channel用于通知KV server的RPC线程何时解除阻塞，由于log的index会越来越大，必须及时清理不需要的channel，这里每个channel只会发送接收一次数据，所以接收后就可以清理了。

4. server与raft的snapshot交互方法是：如果server认为log太长，就会保存当前的状态，保存成功后要求raft层删除所有log；如果leader认为follower落后太多就会把自己最近的snapshot发给follower的raft层，follower收到后不能盲目保存和删除日志，要通过apply channel通知上层server恢复snapshot到状态机，恢复成功后，上层再通知其raft层删除对应日志，并保存snapshot。

5. 由于snapshot和command共用apply channel与上层通信，所以snapshot信息可能会与command交错传递给上层server，导致snapshot已经被apply，而过期的command还在外进传，所以要在server端添加过期判断。

6. 删除log的时候要留一个dummy节点，该节点所在的index是snapshot中最后一个被apply的command。

7. 既可以在AppendEntries的RPC接收端判断是否需要接收snapshot，也可以在发送端判断是否需要发送snapshot，这里为了便于阅读，将判断放在发送端。

8. server将log应用到状态机的过程是不阻塞接收客户端请求的（raft的Start调用不加server的锁），从而提高了并发性能

9. 从channel中发送和接收数据时，不要持有锁，避免发生死锁。

## Lab4 Sharded Key/Value Service

- [x] Shardmaster, a config server with replications to keep fault-tolerant based on Lab3
- [x] Clients can execute Query, Join, Move and Leave commands to group(a set of replica)
- [x] A shardKV server with replications
- [x] ShardKV can query new configuration from configuration server
- [x] Working on unreliable networks, restart and concurrent operations
- [x] ShardKV can provide service on the specific shard according to the configuration
- [x] Supporting shard migration and deletion
- [x] Continue serving shards that are not affected by the ongoing configuration change
- [x] Start serving shards once they are able to, even if the configuration is still ongoing
- [ ] Lease read

并发测试10000次无FAIL

**仍需改进的地方：**

1. 读请求依然走raft流程，no-op+ReadIndex+Lease是优化思路。

2. 迁移数据采用的是pushing模式，但是pulling模式能减少GCing的状态，让结构更清晰。

3. 迁移使用的是轮询，占用一些CPU资源，后续可以改为阻塞模式。