package kvraft

import (
	"ds/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int
	clientId  int64
	commandId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.commandId = 0
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) CommandExcute(args *CommandArgs) string {

	args.CommandId = ck.commandId
	args.ClientId = ck.clientId
	var reply CommandReply
	for {
		DPrintf("[C%v]: send command %v to [S%v]\n", ck.clientId, args, ck.leaderId)
		success := ck.servers[ck.leaderId].Call("KVServer.Command", args, &reply)

		if !success || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// select a random kv server to send rpc
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		DPrintf("[C%v]: receive %v %v from %v commandId %v\n", ck.clientId, reply.Err, reply.Value, ck.leaderId, ck.commandId)
		ck.commandId += 1
		return reply.Value
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.CommandExcute(&CommandArgs{Key: key, Op: "Get"})
}

func (ck *Clerk) Put(key string, value string) {
	ck.CommandExcute(&CommandArgs{Key: key, Value: value, Op: "Put"})
}
func (ck *Clerk) Append(key string, value string) {
	ck.CommandExcute(&CommandArgs{Key: key, Value: value, Op: "Append"})
}
