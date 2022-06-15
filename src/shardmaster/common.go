package shardmaster

import "fmt"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c *Config) String() string {
	return fmt.Sprintf("Num: %v Shards {%v} Groups {%v}", c.Num, c.Shards, c.Groups)
}

func DefaultConfig() Config {
	return Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
}

const (
	OK             = "OK"
	ErrNoConfig    = "ErrNoConfig"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

const (
	JoinOp  = "Join"
	LeaveOp = "Leave"
	MoveOp  = "Move"
	QueryOp = "Query"
)

type OpTypeName string

type CommandArgs struct {
	CommandId int
	ClientId  int64
	Servers   map[int][]string // new GID -> servers mappings
	Num       int              // desired config number
	Shard     int
	GID       int
	GIDs      []int
	OpType    OpTypeName // "Join" "Leave" "Move" and "Query"
}

type CommandReply struct {
	Err    Err
	Config Config
}

func (c *CommandReply) String() string {
	return fmt.Sprintf("Err: %v Config: %v", c.Err, &c.Config)
}
