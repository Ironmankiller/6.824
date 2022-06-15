package shardkv

// import "../shardmaster"
import (
	"ds/labrpc"
	"ds/shardmaster"
	"sort"
	"sync/atomic"
	"time"
)
import "ds/raft"
import "sync"
import "ds/labgob"

const (
	RaftWaitTime = 1300 // ms

	NeedSnapshotDetectInterval = 50

	ConfigurationDetectInterval    = 50 // ms
	MigrateShardDetectInterval     = 80
	GarbageCleanDetectInterval     = 100
	CurrentTermEntryDetectInterval = 300
)

type CommandContext struct {
	CommandId int
	LastReply CommandReply
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	sc           *shardmaster.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastConfig    shardmaster.Config
	currentConfig shardmaster.Config

	lastApplied   int
	statemachines map[int]*Shard // shardId -> shard
	notifier      map[int]chan *CommandReply
	lastResult    map[int64]CommandContext
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	if kv.needSnapshot() {
		DPrintf("[S%v][G%v]: needSnapshot", kv.me, kv.gid)
		kv.mu.Lock()
		lastApplied := kv.lastApplied
		kv.mu.Unlock()
		kv.snapshot(lastApplied)
	}
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) snapshotDetect(timeout int) {
	for kv.killed() == false {
		// Check whether persisted Raft state grows too large
		if kv.needSnapshot() {
			DPrintf("[S%v][G%v]: WeneedSnapshot", kv.me, kv.gid)
			kv.mu.Lock()
			lastApplied := kv.lastApplied
			kv.mu.Unlock()
			kv.snapshot(lastApplied)
		}
		time.Sleep(time.Duration(timeout) * time.Millisecond)
	}
}

func (kv *ShardKV) applier() {
	for kv.killed() == false {
		log := <-kv.applyCh

		if log.CommandValid {

			kv.mu.Lock()
			if log.CommandIndex <= kv.lastApplied {
				DPrintf("[S%v]: discard outdate commandIndex %v lastApplied %v", kv.me, log.CommandIndex, kv.lastApplied)
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = log.CommandIndex
			kv.mu.Unlock()

			DPrintf("[S%v][G%v]: command applied %v\n", kv.me, kv.gid, log.Command.(Command))
			command := log.Command.(Command)
			var reply CommandReply
			switch command.Op {
			case Operation:
				operation := command.Data.(OperationArgs)
				reply = kv.processKVOperation(&operation)
			case Configuration:
				config := command.Data.(shardmaster.Config)
				reply = kv.processConfiguration(&config)
			case InsertShards:
				pullReply := command.Data.(MigrateReply)
				reply = kv.processInsertShards(&pullReply)
			case DeleteShards:
				deleteRequest := command.Data.(MigrateRequest)
				reply = kv.processDeleteShards(&deleteRequest)
			case EmptyEntry:
				reply = kv.processEmptryEntry()
			}

			if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == log.CommandTerm {
				DPrintf("[S%v][G%v]: notify %v\n", kv.me, kv.gid, log.CommandIndex)
				c := kv.getNotifier(log.CommandIndex)
				c <- &reply
			}

			if kv.needSnapshot() {
				DPrintf("[S%v][G%v]: needSnapshot CommandIndex %v", kv.me, kv.gid, log.CommandIndex)
				kv.snapshot(log.CommandIndex)
			}

		} else if log.SnapshotValid {
			DPrintf("[S%v][G%v]: snapshot applied %v\n", kv.me, kv.gid, log)
			//fmt.Printf("[S%v][G%v]: snapshot applied %v\n", kv.me, kv.gid, log)
			kv.processSnapshotValid(log)
		}

	}
}

func (kv *ShardKV) OperationRequest(args *OperationArgs, reply *CommandReply) {

	lastReply, ok := kv.getDuplicate(args.ClientId, args.CommandId, args.Op)
	if ok {
		reply.Err, reply.Value = lastReply.Err, lastReply.Value
		DPrintf("[S%v][G%v]: getDuplicate %v", kv.me, kv.gid, reply)
		return
	}

	if !kv.isNormal(key2shard(args.Key)) {
		kv.mu.Lock()
		DPrintf("[S%v][G%v]: shard %v gid %v current config %v\n", kv.me, kv.gid, key2shard(args.Key), kv.currentConfig.Shards[key2shard(args.Key)], kv.currentConfig)
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	DPrintf("[S%v][G%v]: command %v started1\n", kv.me, kv.gid, args)

	kv.executeRaft(NewOperationCommand(args), reply)
}

func (kv *ShardKV) processKVOperation(command *OperationArgs) CommandReply {
	shardID := key2shard(command.Key)

	if kv.isNormal(shardID) {
		// detect duplication log which has been applied by old leader.
		reply, ok := kv.getDuplicate(command.ClientId, command.CommandId, command.Op)
		if !ok {
			kv.mu.Lock()
			reply = kv.applyLogToStateMachineL(command, shardID)
			if command.Op != GetOp {
				kv.lastResult[command.ClientId] = CommandContext{command.CommandId, reply}
			}
			kv.mu.Unlock()
		} else {
			DPrintf("[S%v]: duplicate %v, not apply to statemachine %v\n", kv.me, command, command.CommandId)
		}
		return reply
	}
	return CommandReply{ErrWrongGroup, ""}
}

func (kv *ShardKV) applyLogToStateMachineL(args *OperationArgs, shardId int) CommandReply {
	var reply CommandReply

	switch args.Op {
	case GetOp:
		reply.Value, reply.Err = kv.statemachines[shardId].Get(args.Key)
	case PutOp:
		reply.Err = kv.statemachines[shardId].Put(args.Key, args.Value)
	case AppendOp:
		reply.Err = kv.statemachines[shardId].Append(args.Key, args.Value)
	}

	DPrintf("[S%v][G%v]: %v command apply to state machine with %v\n", kv.me, kv.gid, args, reply)
	return reply
}

func (kv *ShardKV) configurationTask() {
	canProcessNextConfig := true
	kv.mu.Lock()
	for _, shard := range kv.statemachines {
		if shard.Statue != Normal {
			canProcessNextConfig = false
			DPrintf("[S%v][G%v]: cannot process next config because shard status is %v current config is %v\n", kv.me, kv.gid, kv.getShardStatus(), kv.currentConfig)
			break
		}
	}
	currentConfigNum := kv.currentConfig.Num
	kv.mu.Unlock()

	if canProcessNextConfig {
		next := kv.sc.Query(currentConfigNum + 1)
		// must must apply new config one by one.
		if next.Num == currentConfigNum+1 {
			kv.mu.Lock()
			DPrintf("[S%v][G%v]: found new config %v while current config is %v last config is %v\n", kv.me, kv.gid, next, kv.currentConfig, kv.lastConfig)
			kv.mu.Unlock()
			kv.executeRaft(NewConfigurationCommand(&next), &CommandReply{})
		}
	}
}

func (kv *ShardKV) processConfiguration(nextConfig *shardmaster.Config) CommandReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.currentConfig.Num+1 == nextConfig.Num {
		DPrintf("[S%v][G%v]: update config from %v to %v\n", kv.me, kv.gid, kv.currentConfig, nextConfig)
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *nextConfig
		return CommandReply{OK, ""}
	}
	return CommandReply{ErrOutDate, ""}
}

func (kv *ShardKV) updateShardStatus(next *shardmaster.Config) {

	for sid, gid := range next.Shards {
		if gid == kv.gid {
			_, ok := kv.statemachines[sid]
			if !ok {
				kv.statemachines[sid] = NewShard()
			}
		}
	}
	for shardId := range kv.statemachines {
		if kv.currentConfig.Shards[shardId] == 0 {
			kv.statemachines[shardId].Statue = Normal
			continue
		}
		if kv.currentConfig.Shards[shardId] == kv.gid && next.Shards[shardId] != kv.gid {
			kv.statemachines[shardId].Statue = BePulled
		}
		if kv.currentConfig.Shards[shardId] != kv.gid && next.Shards[shardId] == kv.gid {
			kv.statemachines[shardId].Statue = Pulling
		}
	}
	DPrintf("[S%v][G%v]: after update, status is %v", kv.me, kv.gid, kv.getShardStatus())
}

func (kv *ShardKV) migrateShardTask() {

	kv.mu.Lock()
	gid2shardIds := kv.getShardSliceByStatus(Pulling)
	var wg sync.WaitGroup
	for gid, shardIds := range gid2shardIds {
		wg.Add(1)
		DPrintf("[S%v][G%v]: send pulling shard request to gid %v current config: %v\n", kv.me, kv.gid, gid, kv.currentConfig)
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			pullRequest := MigrateRequest{configNum, shardIds}
			for i := range servers {
				var pullReply MigrateReply
				end := kv.makeEnd(servers[i])
				if end.Call("ShardKV.GetShard", &pullRequest, &pullReply) && pullReply.Err == OK {
					DPrintf("[S%v][G%v]: Get shard data %v with request %v\n", kv.me, kv.gid, pullReply, pullRequest)
					kv.executeRaft(NewInsertShardsCommand(&pullReply), &CommandReply{})
				}
			}

		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIds)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) GetShard(args *MigrateRequest, reply *MigrateReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	defer DPrintf("[S%v][G%v]: process GetShard %v with reply %v\n", kv.me, kv.gid, args, reply)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// kv.currentConfig.Num > args.ConfigNum is impossible,
	// because we can't update configuration until all shards have been migrated successfully
	// kv.currentConfig.Num < args.ConfigNum is possible,
	// because the new configuration may not arrive this group while other group is already pulling new shards
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	reply.Shards = make(map[int]map[string]string)
	for _, sharId := range args.ShardIds {
		reply.Shards[sharId] = kv.statemachines[sharId].deepCopyKV()
	}

	reply.LastResult = make(map[int64]CommandContext)
	for clientId, context := range kv.lastResult {
		reply.LastResult[clientId] = CommandContext{context.CommandId, context.LastReply}
	}

	reply.Err, reply.ConfigNum = OK, args.ConfigNum
}

func (kv *ShardKV) processInsertShards(reply *MigrateReply) CommandReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if reply.ConfigNum == kv.currentConfig.Num {
		DPrintf("[S%v][G%v]: before migrate shards status are %v", kv.me, kv.gid, kv.getShardStatus())
		for shardId, shardMap := range reply.Shards {
			shard := kv.statemachines[shardId]
			if shard.Statue == Pulling {
				for k, v := range shardMap {
					shard.Table[k] = v
				}
				shard.Statue = GCing
			} else {
				DPrintf("[S%v][G%v]: duplicate shard insert", kv.me, kv.gid)
				break
			}
		}
		for clientId, context := range reply.LastResult {
			if result, ok := kv.lastResult[clientId]; !ok || ok && result.CommandId < context.CommandId {
				kv.lastResult[clientId] = context
			}
		}
		DPrintf("[S%v][G%v]: after migrate shards status are %v", kv.me, kv.gid, kv.getShardStatus())
		return CommandReply{OK, ""}
	}
	DPrintf("[S%v][G%v]: reject outdated shards %v current num is %v reply num is %v\n", kv.me, kv.gid, reply, kv.currentConfig.Num, reply.ConfigNum)
	return CommandReply{ErrOutDate, ""}
}

func (kv *ShardKV) getShardStatus() []ShardStatus {
	var key []int
	for i := range kv.statemachines {
		key = append(key, i)
	}

	sort.Ints(key)

	var res []ShardStatus
	for _, i := range key {
		res = append(res, kv.statemachines[i].Statue)
	}

	return res
}

func (kv *ShardKV) getShardLength() []int {
	var key []int
	for i := range kv.statemachines {
		key = append(key, i)
	}

	sort.Ints(key)

	var res []int
	for _, i := range key {
		res = append(res, len(kv.statemachines[i].Table))
	}
	return res
}

func (kv *ShardKV) getShardSliceByStatus(status ShardStatus) map[int][]int {
	res := make(map[int][]int)
	for shardId := range kv.statemachines {
		if kv.statemachines[shardId].Statue == status {
			shardGid := kv.lastConfig.Shards[shardId]
			if _, ok := res[shardGid]; !ok {
				res[shardGid] = make([]int, 0)
			}
			res[shardGid] = append(res[shardGid], shardId)
		}
	}
	return res
}

func (kv *ShardKV) garbageCleanTask() {
	kv.mu.Lock()
	gid2shardIds := kv.getShardSliceByStatus(GCing)
	var wg sync.WaitGroup
	for gid, shardIds := range gid2shardIds {
		wg.Add(1)
		DPrintf("[S%v][G%v]: send delete shard request to gid %v current config: %v\n", kv.me, kv.gid, gid, kv.currentConfig)
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			deleteRequest := MigrateRequest{configNum, shardIds}
			for i := range servers {
				var deleteReply MigrateReply
				end := kv.makeEnd(servers[i])
				if end.Call("ShardKV.DeleteShard", &deleteRequest, &deleteReply) && deleteReply.Err == OK {
					DPrintf("[S%v][G%v]: delete shard data %v with request %v\n", kv.me, kv.gid, deleteReply, deleteRequest)
					kv.executeRaft(NewDeleteShardsCommand(&deleteRequest), &CommandReply{})
				}
			}

		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIds)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) DeleteShard(args *MigrateRequest, reply *MigrateReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	defer DPrintf("[S%v][G%v]: process DeleteShard %v with reply %v\n", kv.me, kv.gid, args, reply)

	kv.mu.Lock()
	if kv.currentConfig.Num > args.ConfigNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var raftRelpy CommandReply
	kv.executeRaft(NewDeleteShardsCommand(args), &raftRelpy)

	reply.Err = raftRelpy.Err
}

func (kv *ShardKV) processDeleteShards(request *MigrateRequest) CommandReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if request.ConfigNum == kv.currentConfig.Num {
		DPrintf("[S%v][G%v]: before delete shards status are %v shard len are %v", kv.me, kv.gid, kv.getShardStatus(), kv.getShardLength())
		for _, shardId := range request.ShardIds {
			shard := kv.statemachines[shardId]
			if shard.Statue == GCing {
				shard.Statue = Normal
			} else if shard.Statue == BePulled {
				kv.statemachines[shardId] = NewShard()
			} else {
				DPrintf("[S%v][G%v]: duplicate delete shard %v current num is %v request num is %v\n", kv.me, kv.gid, request.ShardIds, kv.currentConfig.Num, request.ConfigNum)
				break
			}
		}
		DPrintf("[S%v][G%v]: after delete shards status are %v shard len are %v", kv.me, kv.gid, kv.getShardStatus(), kv.getShardLength())
		return CommandReply{OK, ""}
	}
	DPrintf("[S%v][G%v]: duplicate delete shard %v current num is %v request num is %v\n", kv.me, kv.gid, request.ShardIds, kv.currentConfig.Num, request.ConfigNum)
	return CommandReply{OK, ""}
}

func (kv *ShardKV) currentTermEntryCheckTask() {
	if !kv.rf.HasCurrentTermLog() {
		kv.executeRaft(NewEmptyEntryCommand(), &CommandReply{})
	}
}

func (kv *ShardKV) processEmptryEntry() CommandReply {
	return CommandReply{OK, ""}
}

func (kv *ShardKV) executeRaft(command *Command, reply *CommandReply) {
	index, _, isleader := kv.rf.Start(*command)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[S%v][G%v]: command %v started\n", kv.me, kv.gid, command)

	c := kv.getNotifier(index)

	if _, ok := command.Data.(OperationArgs); ok {
		select {
		case temp := <-c:
			reply.Err, reply.Value = temp.Err, temp.Value
		case <-time.After(RaftWaitTime * time.Millisecond): // timeout is necessary, since request may be sent to minority
			reply.Err = ErrTimeout
		}
	} else {
		temp := <-c
		reply.Err, reply.Value = temp.Err, temp.Value
	}

	go kv.recycleNotifier(index)

	opArgs, ok := command.Data.(OperationArgs)
	if ok {
		DPrintf("[S%v][G%v]: reply %v to %v\n", kv.me, kv.gid, reply, opArgs.ClientId)
	}
	return
}

func (kv *ShardKV) isNormal(shardID int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.gid == kv.currentConfig.Shards[shardID] && (kv.statemachines[shardID].Statue == Normal || kv.statemachines[shardID].Statue == GCing)
}

func (kv *ShardKV) getDuplicate(clientId int64, commandId int, op OpTypeName) (CommandReply, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ctx, ok := kv.lastResult[clientId]
	if ok && ctx.CommandId == commandId && op != GetOp {
		return ctx.LastReply, true
	}
	return CommandReply{}, false
}

// getNotifier Return a channel used in rpc and applier thread
func (kv *ShardKV) getNotifier(index int) chan *CommandReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	c, ok := kv.notifier[index]
	if !ok {
		c = make(chan *CommandReply)
		kv.notifier[index] = c
	}
	return c
}

// recycleNotifierL An channel can only be used once, release the memory occupied by used channel
func (kv *ShardKV) recycleNotifier(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shorterNotifier := make(map[int]chan *CommandReply)
	for k, v := range kv.notifier {
		if k > index {
			shorterNotifier[k] = v
		}
	}
	kv.notifier = shorterNotifier
}

func (kv *ShardKV) leaderTaskTread(task func(), timeout int) {
	for kv.killed() == false {
		if _, isleader := kv.rf.GetState(); isleader {
			task()
		}
		time.Sleep(time.Duration(timeout) * time.Millisecond)
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(OperationArgs{})
	labgob.Register(Command{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(MigrateReply{})
	labgob.Register(MigrateRequest{})

	applyCh := make(chan raft.ApplyMsg)

	kv := &ShardKV{
		me:            me,
		rf:            raft.Make(servers, me, persister, applyCh),
		applyCh:       applyCh,
		dead:          0,
		maxraftstate:  maxraftstate,
		makeEnd:       makeEnd,
		gid:           gid,
		sc:            shardmaster.MakeClerk(masters),
		currentConfig: shardmaster.DefaultConfig(),
		lastConfig:    shardmaster.DefaultConfig(),
		lastApplied:   0,
		statemachines: make(map[int]*Shard),
		notifier:      make(map[int]chan *CommandReply),
		lastResult:    make(map[int64]CommandContext),
	}

	kv.rf.SetGid(gid)

	kv.applySnapshot(persister.ReadSnapshot())
	DPrintf("[S%v][G%v]: state machine restore", kv.me, kv.rf.GetGid())
	go kv.applier()
	//go kv.snapshotDetect(NeedSnapshotDetectInterval)

	go kv.leaderTaskTread(kv.configurationTask, ConfigurationDetectInterval)
	go kv.leaderTaskTread(kv.migrateShardTask, MigrateShardDetectInterval)
	go kv.leaderTaskTread(kv.garbageCleanTask, GarbageCleanDetectInterval)
	go kv.leaderTaskTread(kv.currentTermEntryCheckTask, CurrentTermEntryDetectInterval)

	return kv
}
