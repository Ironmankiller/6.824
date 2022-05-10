package kvraft

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key string, value string) Err
	Append(key string, value string) Err
}

type MemKV struct {
	Table map[string]string
}

// NewMemoryKV must return pointer
func NewMemoryKV() *MemKV {
	return &MemKV{make(map[string]string)}
}

func (mkv *MemKV) Get(key string) (string, Err) {
	value, ok := mkv.Table[key]
	if !ok {
		return "", ErrNoKey
	}
	return value, OK
}

func (mkv *MemKV) Put(key string, value string) Err {
	mkv.Table[key] = value
	return OK
}

func (mkv *MemKV) Append(key string, value string) Err {
	mkv.Table[key] += value
	return OK
}
