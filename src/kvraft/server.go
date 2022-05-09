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
	WaitTime = 2000 // ms
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
	kvtable    map[string]string
	notifier   map[int]chan *CommandReply
	lastResult map[int64]CommandContext
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {

	kv.mu.Lock()
	ctx, ok := kv.lastResult[args.ClientId]
	kv.mu.Unlock()

	if ok && ctx.commandId == args.CommandId {
		reply.Value, reply.Err = ctx.lastReply.Value, ctx.lastReply.Err
		return
	}
	index, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[S%v]: command %v started\n", kv.me, args)

	kv.mu.Lock()
	c := make(chan *CommandReply)
	kv.notifier[index] = c

	kv.mu.Unlock()
	DPrintf("[S%v]: started wait %v at %v\n", kv.me, args, index)

	select {
	case temp := <-c:
		reply.Err, reply.Value = temp.Err, temp.Value
	case <-time.After(WaitTime * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go func(index int) {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		shorterNotifier := make(map[int]chan *CommandReply)
		for k, v := range kv.notifier {
			if k > index {
				shorterNotifier[k] = v
			}
		}
		kv.notifier = shorterNotifier
	}(index)

	DPrintf("[S%v]: reply %v to %v\n", kv.me, reply, args.ClientId)
	return
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		log := <-kv.applyCh
		DPrintf("[S%v]: applied %v\n", kv.me, log.Command.(CommandArgs))

		if !log.CommandValid {
			continue
		}
		command := log.Command.(CommandArgs)

		kv.mu.Lock()
		var reply CommandReply
		ctx, ok := kv.lastResult[command.ClientId]
		if ok && ctx.commandId == command.CommandId {
			DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.me, command, kv.lastResult[command.ClientId], command.ClientId)
			reply = ctx.lastReply
		} else {
			reply = kv.applyLogToStateMachine(command)
			kv.lastResult[command.ClientId] = CommandContext{command.CommandId, reply}
		}
		c := kv.notifier[log.CommandIndex]
		kv.mu.Unlock()

		currentTerm, isLeader := kv.rf.GetState()
		DPrintf("[S%v]: isLeader %v currentTerm %v CommandTerm %v\n", kv.me, isLeader, currentTerm, log.CommandTerm)
		if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == log.CommandTerm {
			DPrintf("[S%v]: notify %v\n", kv.me, log.CommandIndex)
			c <- &reply
		}
	}
}

func (kv *KVServer) applyLogToStateMachine(args CommandArgs) CommandReply {
	var reply CommandReply
	reply.Err = OK
	reply.Value = ""

	switch args.Op {
	case GetOp:
		var ok bool
		reply.Value, ok = kv.kvtable[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		}
	case PutOp:
		kv.kvtable[args.Key] = args.Value
	case AppendOp:
		kv.kvtable[args.Key] += args.Value
	}
	DPrintf("[S%v]: %v command apply to state machine with %v\n", kv.me, args, reply)
	return reply
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
		kvtable:      make(map[string]string),
		notifier:     make(map[int]chan *CommandReply),
		lastResult:   make(map[int64]CommandContext),
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()

	return kv
}
