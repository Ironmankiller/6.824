package shardkv

import (
	"ds/shardmaster"
	"fmt"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrOutDate     = "ErrOutDate"
	ErrNotReady    = "ErrNotReady"
)

type Err string

type OpTypeName int8

const (
	GetOp    OpTypeName = iota
	PutOp               = iota
	AppendOp            = iota
)

type OperationArgs struct {
	Key       string
	Value     string
	Op        OpTypeName // "Get" "Put" or "Append"
	ClientId  int64
	CommandId int
}

func (a *OperationArgs) String() string {
	return fmt.Sprintf("{Key: %v, Value: %v} Op: %v ClientId: %v CommandId: %v\n", a.Key, a.Value, a.Op, a.ClientId, a.CommandId)
}

type MigrateRequest struct {
	ConfigNum int
	ShardIds  []int
}

type MigrateReply struct {
	Err        Err
	ConfigNum  int
	Shards     map[int]map[string]string
	LastResult map[int64]CommandContext
}

func (mr *MigrateReply) String() string {
	return fmt.Sprintf("Err: %v ConfigNum: %v\n", mr.Err, mr.ConfigNum)
}

type CommandType int8

const (
	Operation CommandType = iota
	Configuration
	InsertShards
	DeleteShards
	EmptyEntry
)

type Command struct {
	Op   CommandType
	Data interface{}
}

type CommandReply struct {
	Err   Err
	Value string
}

func (c *Command) String() string {
	return fmt.Sprintf("Type: %v, Data: %v", c.Op, c.Data)
}

func NewOperationCommand(args *OperationArgs) *Command {
	return &Command{Operation, *args}
}

func NewConfigurationCommand(args *shardmaster.Config) *Command {
	return &Command{Configuration, *args}
}

func NewInsertShardsCommand(args *MigrateReply) *Command {
	return &Command{InsertShards, *args}
}

func NewDeleteShardsCommand(args *MigrateRequest) *Command {
	return &Command{DeleteShards, *args}
}

func NewEmptyEntryCommand() *Command {
	return &Command{EmptyEntry, nil}
}
