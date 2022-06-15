package shardkv

import (
	"bytes"
	"ds/labgob"
	"ds/raft"
	"ds/shardmaster"
)

func (kv *ShardKV) processSnapshotValid(log raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[S%v]: processSnapshotValid\n", kv.me)
	if kv.rf.ApplySnapshot(log.SnapshotData, log.SnapshotTerm, log.SnapshotIndex) {
		kv.applySnapshot(log.SnapshotData)
		kv.lastApplied = log.SnapshotIndex
	}
}

func (kv *ShardKV) applySnapshot(data []byte) {

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	statemachines := make(map[int]*Shard)
	lastResult := make(map[int64]CommandContext)
	lastConfig := shardmaster.DefaultConfig()
	currentConfig := shardmaster.DefaultConfig()
	if d.Decode(&statemachines) != nil ||
		d.Decode(&lastResult) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&currentConfig) != nil {
		DPrintf("[S%v]: decode snapshot false %v %v", kv.me, statemachines, data)
		panic("decode false")
	} else {
		kv.statemachines = statemachines
		kv.lastResult = lastResult
		kv.currentConfig = currentConfig
		kv.lastConfig = lastConfig
		DPrintf("[S%v][G%v]: apply snapshot status: %v length: %v currentConfig: %v lastConfig: %v lastResult: %v\n",
			kv.me, kv.gid, kv.getShardStatus(), kv.getShardLength(), currentConfig, lastConfig, lastResult)
	}
}

func (kv *ShardKV) snapshot(commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	DPrintf("[S%v][G%v]: state machine status %v length %v\n", kv.me, kv.gid, kv.getShardStatus(), kv.getShardLength())
	if err := e.Encode(kv.statemachines); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.lastResult); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.lastConfig); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.currentConfig); err != nil {
		panic(err)
	}
	kv.rf.SnapShot(commandIndex, w.Bytes())

}

func (kv *ShardKV) needSnapshot() bool {
	isNeed := kv.maxraftstate != -1 && kv.rf.GetLogLength() > int(0.1*float32(kv.maxraftstate))
	DPrintf("[S%v][G%v]: raft log %v snapshot %v isNeed %v\n", kv.me, kv.gid, kv.rf.GetLogLength(), kv.rf.GetSnapshotLength(), isNeed)
	return isNeed
}
