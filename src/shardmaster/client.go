package shardmaster

//
// Shardmaster clerk.
//

import "ds/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	return ck
}

func (ck *Clerk) CommandRequest(args *CommandArgs) Config {
	args.CommandId = ck.commandId
	args.ClientId = ck.clientId
	var reply CommandReply
	for {
		DPrintf("[C%v]: send command %v to [S%v]\n", ck.clientId, args, ck.leaderId)
		success := ck.servers[ck.leaderId].Call("ShardMaster.Command", args, &reply)

		if !success || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// select a random kv server to send rpc
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		DPrintf("[C%v]: receive %v from %v commandId %v\n", ck.clientId, reply, ck.leaderId, ck.commandId)
		ck.commandId += 1
		return reply.Config
	}
}

func (ck *Clerk) Query(num int) Config {
	return ck.CommandRequest(&CommandArgs{Num: num, OpType: "Query"})
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.CommandRequest(&CommandArgs{Servers: servers, OpType: "Join"})
}

func (ck *Clerk) Leave(gids []int) {
	ck.CommandRequest(&CommandArgs{GIDs: gids, OpType: "Leave"})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.CommandRequest(&CommandArgs{GID: gid, Shard: shard, OpType: "Move"})
}
