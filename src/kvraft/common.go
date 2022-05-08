package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

const (
	GetOp    = "Get"
	PutOp    = "Put"
	AppendOp = "Append"
)

type OpTypeName string

// Put or Append
type CommandArgs struct {
	Key       string
	Value     string
	Op        OpTypeName // "Get" "Put" or "Append"
	ClientId  int64
	CommandId int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type CommandReply struct {
	Err   Err
	Value string
}
