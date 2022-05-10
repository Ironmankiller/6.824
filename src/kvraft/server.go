package kvraft

import (
	"bytes"
	"ds/labgob"
	"ds/labrpc"
	"ds/raft"
	"sync"
	"sync/atomic"
	"time"
)

const (
	WaitTime = 1300 // ms
)

type CommandContext struct {
	CommandId int
	LastReply CommandReply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied  int

	// Your definitions here.
	statemachine KVStateMachine
	notifier     map[int]chan *CommandReply
	lastResult   map[int64]CommandContext
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {

	// detect duplication to avoid raft process
	lastReply, ok := kv.checkDuplicate(args.ClientId, args.CommandId, args.Op)
	if ok {
		reply.Err, reply.Value = lastReply.Err, lastReply.Value
		return
	}

	index, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[S%v]: command %v started\n", kv.me, args)

	c := kv.getNotifier(index)

	select {
	case temp := <-c:
		reply.Err, reply.Value = temp.Err, temp.Value
	case <-time.After(WaitTime * time.Millisecond): // timeout is necessary, since request may be sent to minority
		reply.Err = ErrTimeout
	}

	go kv.recycleNotifierL(index)

	DPrintf("[S%v]: reply %v to %v\n", kv.me, reply, args.ClientId)
	return
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		log := <-kv.applyCh

		if log.CommandValid {
			DPrintf("[S%v]: command applied %v\n", kv.me, log.Command.(CommandArgs))
			kv.processCommandValid(log)
		} else if log.SnapshotValid {
			DPrintf("[S%v]: snapshot applied %v\n", kv.me, log)
			kv.processSnapshotValid(log)
		}

	}
}

func (kv *KVServer) processCommandValid(log raft.ApplyMsg) {
	command := log.Command.(CommandArgs)

	if log.CommandIndex <= kv.lastApplied {
		DPrintf("[S%v]: discard outdate commandIndex %v lastApplied %v", kv.me, log.CommandIndex, kv.lastApplied)
		return
	}
	kv.lastApplied = command.CommandId

	// detect duplication log which has been applied by old leader.
	reply, ok := kv.checkDuplicate(command.ClientId, command.CommandId, command.Op)
	if !ok {
		kv.mu.Lock()
		reply = kv.applyLogToStateMachineL(command)
		if command.Op != GetOp {
			kv.lastResult[command.ClientId] = CommandContext{command.CommandId, reply}
		}
		kv.mu.Unlock()
	}

	// Check "isLeader==true" means only notify rpc thread whose client is waited on leader server.
	// "isLeader==false" happens in server lose its leadership before apply. In this case, the rpc will
	// return a timeout error, and client will choose a new server to retry.
	// Check "currentTerm == log.CommandTerm" means the leader server only be allowed to notify rpc thread whose
	// command is started at its term
	// "currentTerm != log.CommandTerm" happens in server lose its leadership but becomes leader again before
	// rpc return timeout error. in the case, server's log may be applied by other leader before it become
	// leader again. So All clients waiting on old leader will get timeout error, and redirect their requests
	// to new leader.
	// Notice that if election timeout is larger than client request timeout, "currentTerm != log.CommandTerm"
	// will never occur, but someone may change election timeout in the future. Just to be safe side, we have to
	// check "currentTerm == log.CommandTerm"
	if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == log.CommandTerm {
		DPrintf("[S%v]: notify %v\n", kv.me, log.CommandIndex)
		c := kv.getNotifier(log.CommandIndex)
		c <- &reply
	}

	// Check whether persisted Raft state grows too large
	if kv.needSnapshot() {
		DPrintf("[S%v]: needSnapshot", kv.me)
		kv.snapshot(log.CommandIndex)
	}
}

func (kv *KVServer) processSnapshotValid(log raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[S%v]: processSnapshotValid\n", kv.me)
	if kv.rf.ApplySnapshot(log.SnapshotData, log.SnapshotTerm, log.SnapshotIndex) {
		kv.applySnapshot(log.SnapshotData)
		kv.lastApplied = log.SnapshotIndex
	}
}

func (kv *KVServer) applySnapshot(data []byte) {

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	stateMachine := &MemKV{}
	var lastResult map[int64]CommandContext
	if d.Decode(stateMachine) != nil ||
		d.Decode(&lastResult) != nil {
		DPrintf("[S%v]: decode snapshot false %v %v", kv.me, stateMachine, data)
		panic("decode false")
	} else {
		kv.statemachine = stateMachine
		kv.lastResult = lastResult
	}
}

func (kv *KVServer) snapshot(commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	DPrintf("[S%v]: state machine %v", kv.me, kv.statemachine)
	if err := e.Encode(kv.statemachine); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.lastResult); err != nil {
		panic(err)
	}
	kv.rf.SnapShot(commandIndex, w.Bytes())
}

func (kv *KVServer) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.rf.GetLogLength() > int(0.7*float32(kv.maxraftstate))
}

func (kv *KVServer) applyLogToStateMachineL(args CommandArgs) CommandReply {
	var reply CommandReply

	switch args.Op {
	case GetOp:
		reply.Value, reply.Err = kv.statemachine.Get(args.Key)
	case PutOp:
		reply.Err = kv.statemachine.Put(args.Key, args.Value)
	case AppendOp:
		reply.Err = kv.statemachine.Append(args.Key, args.Value)
	}

	DPrintf("[S%v]: %v command apply to state machine with %v\n", kv.me, args, reply)
	return reply
}

func (kv *KVServer) checkDuplicate(clientId int64, commandId int, op OpTypeName) (CommandReply, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ctx, ok := kv.lastResult[clientId]
	if ok && ctx.CommandId == commandId && op != GetOp {
		return ctx.LastReply, true
	}
	return CommandReply{}, false
}

// getNotifier Return a channel used in rpc and applier thread
func (kv *KVServer) getNotifier(index int) chan *CommandReply {
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
func (kv *KVServer) recycleNotifierL(index int) {
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

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(CommandArgs{})
	labgob.Register(MemKV{})
	kv := &KVServer{
		me:           me,
		maxraftstate: maxraftstate,
		statemachine: NewMemoryKV(),
		lastApplied:  0,
		notifier:     make(map[int]chan *CommandReply),
		lastResult:   make(map[int64]CommandContext),
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.applySnapshot(persister.ReadSnapshot())
	DPrintf("[S%v]: state machine restore %v", kv.me, kv.statemachine)
	go kv.applier()

	return kv
}
