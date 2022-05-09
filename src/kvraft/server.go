package kvraft

import (
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
	commandId int
	lastReply CommandReply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	statemachine KVStateMachine
	notifier     map[int]chan *CommandReply
	lastResult   map[int64]CommandContext
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {

	// process duplication
	lastReply, ok := kv.checkDuplicate(args.ClientId, args.CommandId)
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
		command := log.Command.(CommandArgs)
		DPrintf("[S%v]: applied %v\n", kv.me, log.Command.(CommandArgs))

		if !log.CommandValid {
			continue
		}

		// process duplication cased by new leader.
		reply, ok := kv.checkDuplicate(command.ClientId, command.CommandId)
		if !ok {
			kv.mu.Lock()
			reply = kv.applyLogToStateMachineL(command)
			kv.lastResult[command.ClientId] = CommandContext{command.CommandId, reply}
			kv.mu.Unlock()
		}

		// Check whether this server is real leader. (It may lose leadership before commit)
		if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == log.CommandTerm {
			DPrintf("[S%v]: notify %v\n", kv.me, log.CommandIndex)
			c := kv.getNotifier(log.CommandIndex)
			c <- &reply
		}
	}
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

func (kv *KVServer) checkDuplicate(clientId int64, commandId int) (CommandReply, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ctx, ok := kv.lastResult[clientId]
	if ok && ctx.commandId == commandId {
		return ctx.lastReply, true
	}
	return CommandReply{}, false
}

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

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(CommandArgs{})
	labgob.Register(&CommandReply{})
	kv := &KVServer{
		me:           me,
		maxraftstate: maxraftstate,
		statemachine: NewMemoryKV(),
		notifier:     make(map[int]chan *CommandReply),
		lastResult:   make(map[int64]CommandContext),
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()

	return kv
}
