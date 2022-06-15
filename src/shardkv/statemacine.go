package shardkv

import "fmt"

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key string, value string) Err
	Append(key string, value string) Err
}

type ShardStatus uint8

const (
	Normal ShardStatus = iota
	Pulling
	BePulled
	GCing
)

type Shard struct {
	Table  map[string]string
	Statue ShardStatus
}

// NewMemoryKV must return pointer
func NewShard() *Shard {
	return &Shard{
		make(map[string]string),
		Normal,
	}
}

func (skv *Shard) String() string {
	return fmt.Sprintf("Status: %v, Table: %v", skv.Statue, skv.Table)
}

func (skv *Shard) Get(key string) (string, Err) {
	value, ok := skv.Table[key]
	if !ok {
		return "", ErrNoKey
	}
	return value, OK
}

func (skv *Shard) Put(key string, value string) Err {
	skv.Table[key] = value
	return OK
}

func (skv *Shard) Append(key string, value string) Err {
	skv.Table[key] += value
	return OK
}

func (skv *Shard) deepCopyKV() map[string]string {
	ret := make(map[string]string)
	for k := range skv.Table {
		ret[k] = skv.Table[k]
	}
	return ret
}
