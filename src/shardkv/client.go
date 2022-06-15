package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "ds/labrpc"
import "crypto/rand"
import "math/big"
import "ds/shardmaster"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  map[int]int
	clientId  int64
	commandId int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		sm:        shardmaster.MakeClerk(masters),
		make_end:  make_end,
		clientId:  nrand(),
		commandId: 0,
		leaderId:  make(map[int]int),
	}
	ck.config = ck.sm.Query(-1)
	return ck
}

func (ck *Clerk) CommandExecute(args *OperationArgs) string {

	args.CommandId = ck.commandId
	args.ClientId = ck.clientId
	var reply CommandReply

	for {
		shard := key2shard(args.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok := ck.leaderId[gid]; !ok {
				ck.leaderId[gid] = 0
			}
			oldLeaderId := ck.leaderId[gid]
			curLeaderId := ck.leaderId[gid]
			for {
				srv := ck.make_end(servers[curLeaderId])
				DPrintf("[C%v]: send command %v shard %v to [S%v][G%v]\n", ck.clientId, args, shard, curLeaderId, gid)
				ok := srv.Call("ShardKV.OperationRequest", args, &reply)
				DPrintf("[C%v]: receive %v shard %v commandId %v\n", ck.clientId, reply, shard, ck.commandId)
				if ok && (reply.Err == ErrNoKey || reply.Err == OK) {
					ck.commandId++
					ck.leaderId[gid] = curLeaderId
					return reply.Value
				} else if ok && reply.Err == ErrWrongGroup {
					ck.leaderId[gid] = int(nrand()) % len(servers)
					break
				} else {
					curLeaderId = (curLeaderId + 1) % len(servers)
					if curLeaderId == oldLeaderId {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
		DPrintf("[C%v]: query config %v", ck.clientId, ck.config)
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.CommandExecute(&OperationArgs{Key: key, Op: GetOp})
}

func (ck *Clerk) Put(key string, value string) {
	ck.CommandExecute(&OperationArgs{Key: key, Value: value, Op: PutOp})
}
func (ck *Clerk) Append(key string, value string) {
	ck.CommandExecute(&OperationArgs{Key: key, Value: value, Op: AppendOp})
}
