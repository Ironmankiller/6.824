package raft

import "fmt"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	Data              []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (a *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("InstallSnapshotArgs: {Term %v LeaderId %v LastIncludedIndex %v LastIncludedTerm %v}",
		a.Term, a.LeaderId, a.LastIncludedIndex, a.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term int
}

func (a *InstallSnapshotReply) String() string {
	return fmt.Sprintf("InstallSnapshotReply: {Term %v}", a.Term)
}

// SnapShot Invoked by app layer who think the raft log is too long, delete the log before "index" and persist.
func (rf *Raft) SnapShot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index < rf.log.FirstIndex() {
		DPrintf("[S%v][G%v]: error: index is smaller than first index of raft log", rf.me, rf.Gid)
		return
	}
	//fmt.Printf("[S%v]: SnapShot EraseBefore index %v last index %v\n", rf.me, index, rf.log.LastIndex())
	//fmt.Printf("before[S%v]: SnapShot %v log %v\n", rf.me, rf.log.GetLogLength(), rf.persister.RaftStateSize())
	rf.log.EraseBefore(index)
	//fmt.Printf("after[S%v]: SnapShot %v log %v\n", rf.me, rf.log.GetLogLength(), rf.persister.RaftStateSize())
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	//fmt.Printf("save[S%v]: SnapShot %v log %v\n", rf.me, rf.log.GetLogLength(), rf.persister.RaftStateSize())
}

// ApplySnapshot Invoked by app layer to delete corresponding log in raft layer, update raft state and persist.
func (rf *Raft) ApplySnapshot(data []byte, lastTerm int, lastIndex int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Notice that "lastIndex > rf.commitIndex" while "lastIndex < rf.log.FirstIndex()" is possible.
	// In the other word, commit index is smaller than log's first index occasionally.
	// when I ran 2000 times, this case occurred once, I guess This server was restarted, and commit index
	// was 0 at beginning, so it didn't met the "lastIndex <= rf.commitIndex" condition, while its log is
	// newer than snapshot send by leader. So this server must reject this snapshot, otherwise, the panic will
	// be triggered at line 57.
	if lastIndex <= rf.commitIndex || lastIndex < rf.log.FirstIndex() {
		DPrintf("[S%v][G%v]: reject snapshot server commitIndex=%v, snapshot lastIndex=%v", rf.me, rf.me, rf.commitIndex, lastIndex)
		return false
	}

	erase := lastIndex
	if erase > rf.log.LastIndex() {
		erase = rf.log.LastIndex()
	}
	rf.log.EraseBefore(erase)

	firstEntry := Entry{
		Command: nil,
		Term:    lastTerm,
	}
	rf.log.SetFirst(lastIndex, firstEntry)
	rf.lastApplied, rf.commitIndex = lastIndex, lastIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), data)
	DPrintf("[S%v][G%v]: accept snapshot server commitIndex=%v, log %v", rf.me, rf.Gid, lastIndex, rf.log)
	return true
}

// InstallSnapShot Invoked by leader to send chunks of a snapshot to a follower.
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
	}
	rf.role = Follower
	rf.setElectionTimeoutL()

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	rf.waitSnapshotFlag = true
	rf.waitSnapshot = args

	rf.applyCond.Broadcast()
}

func (rf *Raft) handleSnapshotReplyL(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if reply.Term > rf.currentTerm {
		rf.newTermL(reply.Term)
	} else {
		// Some log may be sent many times, but we must keep nextIndex monotonic increasing.
		next := args.LastIncludedIndex + 1
		match := args.LastIncludedIndex
		if next > rf.nextIndex[server] {
			rf.nextIndex[server] = next
		}
		if match > rf.matchIndex[server] {
			rf.matchIndex[server] = match
		}
	}

	DPrintf("[S%v][G%v] get %v ShotShotInstall reply %v next %v", rf.me, rf.Gid, server, reply, rf.nextIndex[server])
}

func (rf *Raft) makeSnapshotInstallArgs() *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.persister.ReadSnapshot(),
		rf.log.FirstIndex(),
		rf.log.GetAt(rf.log.FirstIndex()).Term,
	}
}

func (rf *Raft) sendSnapshotRequest(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}
