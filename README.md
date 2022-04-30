# 6.824 Lab

## Lab1 MapReduce

- [x] Word Count测试
- [x] Indexer测试
- [x] 并行Map测试
- [x] 并行Reduce测试
- [x] Crash恢复测试
- [ ] 多机MapReduce



**仍需改进的地方：**

1. 本代码只能在一台机器上多进程运行

2. 错误检测方法与MapReduce论文中采用心跳检测不同，是直接开启一个线程，在其中等待固定时间内是否收到Worker完成任务的回复，这种错误检测存在的问题是，固定的时间不好设置。因为对于不同任务Worker运行的时间不同，如果等待时间过短，有可能任务还没有执行完就被Master置为IDLE并分配给其他Worker，如果等待时间过长，则不能有效检测出真正崩溃的Worker

3. 由于执行Reduce的Worker和执行Map的Worker可能不是同一台机器，所以Reduce过程中读取中间结果需要采用RPC

4. 错误恢复机制与论文中的方法不同，在论文所述的实现中，Map任务产生的中间结果是保存在本地磁盘的，如果此时存放该中间结果的Worker崩溃，执行Reduce的Worker就无法从中读取中间结果，所以需要重新将崩溃的Worker上完成的所有Map任务重新置为IDLE，然后再分配给其他Worker

## Lab2 Raft

- [x] Leader election
- [x] Append log entries
- [x] Safety
- [x] Persist
- [x] Reduce the number of rejected
- [ ] Figure8 (unreliable)
- [ ] No-op log

**仍需改进的地方：**

1. 本代码的持久化基于实验框架提供的Labgob，没有真正利用磁盘持久化。对于磁盘持久化还需要考虑在持久化的过程中磁盘出现问题导致的数据不一致，可能的解决方法是创建临时文件，写成功后再原子修改文件名；另外还需要考虑写入磁盘的代价，如果用户每次请求的都log都落盘，意味着即使是一个简单的读请求也需要几十毫秒的磁盘IO时间，这显然是不能忍受的，所以可能的解决方法是批量地进行持久化，或者采用更高速的持久化介质，比如SSD或者不断电的内存。

2. Figure8 (unreliable)测试出现过一次Fail，但是后续运行了多次也没有再复现出这个bug，所以还需要进一步分析review代码。

3. 论文中Figure8所表达的情形是，新的leader并不知道之前term的log是否已经被提交，所以当且仅当leader自己term的log被复制到大多数它才允许间接apply其他term的日志，换句话说leader拥有至少在自己任期内提交一个log的能力，它才能下决断提交之前的log，因为如果它有这种能力，在它崩溃后，新的leader将从允许它提交的大多数中选择，而从这些服务器中选出来的leader显然不会重复提交任何index上的log。这种解决方案带来的一个问题就是，如果leader长时间无法将自己term的log复制到大多数（可能是网络问题，也可能是根本没有客户请求），那么即使他能够将之前term的log复制到大多数，它也不被允许提交这些log，这就意味着其他term的客户的请求会被阻塞，所以leader迫切地需要知道之前term的log能否提交。一个可能的解决方案是，在新的leader上任后，发送一个command为空的no-op log，通过测试这个log能否被提交来间接将其他term的log提交。