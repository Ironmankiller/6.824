# 6.824 Lab

#### Lab1

- [x] Word Count测试
- [x] Indexer测试
- [x] 并行Map测试
- [x] 并行Reduce测试
- [x] clash恢复测试
- [ ] 多机MapReduce



**仍需改进的地方：**

1. 本代码只能在一台机器上多进程运行

2. 错误检测方法与MapReduce论文中采用心跳检测不同，是直接开启一个线程，在其中等待固定时间内是否收到Worker完成任务的回复，这种错误检测存在的问题是，固定的时间不好设置。因为对于不同任务Worker运行的时间不同，如果等待时间过短，有可能任务还没有执行完就被Master置为IDLE并分配给其他Worker，如果等待时间过长，则不能有效检测出真正崩溃的Worker

3. 由于执行Reduce的Worker和执行Map的Worker可能不是同一台机器，所以Reduce过程中读取中间结果需要采用RPC

4. 错误恢复机制与论文中的方法不同，在论文所述的实现中，Map任务产生的中间结果是保存在本地磁盘的，如果此时存放该中间结果的Worker崩溃，执行Reduce的Worker就无法从中读取中间结果，所以需要重新将崩溃的Worker上完成的所有Map任务重新置为IDLE，然后再分配给其他Worker
