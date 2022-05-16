package shardmaster

import (
	"ds/labgob"
	"ds/raft"
	"sync/atomic"
	"time"
)
import "ds/labrpc"
import "sync"

const (
	WaitTime = 1300 // ms
)

type CommandContext struct {
	CommandId int
	LastReply CommandReply
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32    // set by Kill()
	configs []Config // indexed by config num

	// Your data here.
	lastApplied  int
	stateMachine ConfigStateMachine
	notifier     map[int]chan *CommandReply
	lastResult   map[int64]CommandContext
}

type Op struct {
	// Your data here.
}

func (sm *ShardMaster) Command(args *CommandArgs, reply *CommandReply) {

	// detect duplication to avoid raft process
	lastReply, ok := sm.checkDuplicate(args.ClientId, args.CommandId, args.OpType)
	if ok {
		reply.Err, reply.Config = lastReply.Err, lastReply.Config
		return
	}

	index, _, isLeader := sm.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[S%v]: command %v started\n", sm.me, args)

	c := sm.getNotifier(index)

	select {
	case temp := <-c:
		reply.Err, reply.Config = temp.Err, temp.Config
	case <-time.After(WaitTime * time.Millisecond): // timeout is necessary, since request may be sent to minority
		reply.Err = ErrTimeout
	}

	go sm.recycleNotifierL(index)

	DPrintf("[S%v]: reply %v to %v\n", sm.me, reply, args.ClientId)
	return

}

func (sm *ShardMaster) applier() {
	for sm.killed() == false {
		log := <-sm.applyCh
		DPrintf("[S%v]: notify %v\n", sm.me, log)
		if log.CommandValid {
			DPrintf("[S%v]: command applied %v\n", sm.me, log.Command.(CommandArgs))
			sm.processCommandValid(log)
		}
	}
}

func (sm *ShardMaster) processCommandValid(log raft.ApplyMsg) {

	command, ok := log.Command.(CommandArgs)

	// This check is useless when snapshot not occurs
	if log.CommandIndex <= sm.lastApplied {
		DPrintf("[S%v]: discard outdate commandIndex %v lastApplied %v", sm.me, log.CommandIndex, sm.lastApplied)
		return
	}
	sm.lastApplied = command.CommandId

	// detect duplication log which has been applied by old leader.
	reply, ok := sm.checkDuplicate(command.ClientId, command.CommandId, command.OpType)
	if !ok {
		sm.mu.Lock()
		reply = sm.applyLogToStateMachineL(command)
		if command.OpType != QueryOp {
			sm.lastResult[command.ClientId] = CommandContext{command.CommandId, reply}
		}
		sm.mu.Unlock()
	} else {
		DPrintf("[S%v]: duplicate %v, not notify %v\n", sm.me, command, log.CommandIndex)
	}

	if currentTerm, isLeader := sm.rf.GetState(); isLeader && currentTerm == log.CommandTerm {
		DPrintf("[S%v]: notify %v\n", sm.me, log.CommandIndex)
		c := sm.getNotifier(log.CommandIndex)
		c <- &reply
	}
}

func (sm *ShardMaster) applyLogToStateMachineL(args CommandArgs) CommandReply {
	var reply CommandReply

	switch args.OpType {
	case JoinOp:
		reply.Err = sm.stateMachine.Join(args.Servers)
	case LeaveOp:
		reply.Err = sm.stateMachine.Leave(args.GIDs)
	case MoveOp:
		reply.Err = sm.stateMachine.Move(args.Shard, args.GID)
	case QueryOp:
		reply.Config, reply.Err = sm.stateMachine.Query(args.Num)
	}

	DPrintf("[S%v]: %v command apply to state machine with %v\n", sm.me, args, reply)
	return reply
}

func (sm *ShardMaster) checkDuplicate(clientId int64, commandId int, op OpTypeName) (CommandReply, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	ctx, ok := sm.lastResult[clientId]
	if ok && ctx.CommandId == commandId && op != QueryOp {
		return ctx.LastReply, true
	}
	return CommandReply{}, false
}

// getNotifier Return a channel used in rpc and applier thread
func (sm *ShardMaster) getNotifier(index int) chan *CommandReply {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	c, ok := sm.notifier[index]
	if !ok {
		c = make(chan *CommandReply)
		sm.notifier[index] = c
	}
	return c
}

// recycleNotifierL An channel can only be used once, release the memory occupied by used channel
func (sm *ShardMaster) recycleNotifierL(index int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	shorterNotifier := make(map[int]chan *CommandReply)
	for k, v := range sm.notifier {
		if k > index {
			shorterNotifier[k] = v
		}
	}
	sm.notifier = shorterNotifier
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	labgob.Register(CommandArgs{})
	labgob.Register(MemConfig{})

	sm := &ShardMaster{
		me:           me,
		configs:      make([]Config, 1),
		lastApplied:  0,
		stateMachine: NewMemConfig(),
		notifier:     make(map[int]chan *CommandReply),
		lastResult:   make(map[int64]CommandContext),
	}

	sm.configs[0].Groups = map[int][]string{}

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go sm.applier()

	return sm
}
