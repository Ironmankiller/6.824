package raft

import (
	"fmt"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

func (a *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Term %v LeaderId %v PrevLogIndex %v PrevLogTerm %v LeaderCommit %v Entries %v",
		a.Term, a.LeaderId, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit, a.Entries)
}

type AppendEntriesReply struct {
	Term          int
	ConflictTerm  int
	ConflictIndex int
	Success       bool
}

func (a *AppendEntriesReply) String() string {
	if a.ConflictTerm == 0 {
		return fmt.Sprintf("Term %v success %v", a.Term, a.Success)
	} else {
		return fmt.Sprintf("Term %v success %v ConflictTerm %v", a.Term, a.Success, a.ConflictTerm)
	}
}

// Respond to RPCs from candidates and leaders
func (rf *Raft) AppendRequest(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// AppendEntries Receiver implementation: 1
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// All Servers: 1
		rf.newTermL(args.Term)
	}

	// 这里有必要设为Follower吗？
	rf.role = Follower
	// Follower: 2
	rf.setElectionTimeoutL()

	if args.PrevLogIndex < rf.log.FirstIndex() {
		reply.Term, reply.Success = rf.currentTerm, true
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, args, args.LeaderId, args.PrevLogIndex, rf.log.FirstIndex())
		return
	}

	reply.Success = false
	DPrintf("[S%v]: wow prevLogIndex %v FirstIndex %v LastIndex %v entries=%v", rf.me, args.PrevLogIndex, rf.log.FirstIndex(), rf.log.LastIndex(), args.Entries)
	if rf.log.LastIndex() < args.PrevLogIndex {
		// AppendEntries Receiver implementation: 2
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.log.LastIndex() + 1
	} else if rf.log.GetAt(args.PrevLogIndex).Term != args.PrevLogTerm { // not matched
		// AppendEntries Receiver implementation: 3
		reply.ConflictTerm = rf.log.GetAt(args.PrevLogIndex).Term

		// get first index of ConflictTerm
		index := args.PrevLogIndex
		for index-1 >= rf.log.FirstIndex() &&
			rf.log.GetAt(index-1).Term == reply.ConflictTerm {
			index -= 1
		}
		reply.ConflictIndex = index
	} else {
		for i, e := range args.Entries {
			entryIndex := args.PrevLogIndex + 1 + i
			// it's time to append
			if entryIndex > rf.log.LastIndex() || !rf.log.GetAt(entryIndex).isEqual(e) {
				//DPrintf("[S%v]: now %v\n", rf.me, args.Entries[i:])
				rf.log.AppendAfterIndex(entryIndex, args.Entries[i:])
				break
			}
		}
		DPrintf("[S%v]: lastIndex %v now %v\n", rf.me, rf.log.LastIndex(), rf.log)
		rf.advanceCommit(args.LeaderCommit)
		reply.Success = true
	}

	reply.Term = rf.currentTerm
	rf.persist()
}

func (rf *Raft) advanceCommit(leaderCommit int) {
	// AppendEntries Receiver implementation: 5
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = leaderCommit
		lastNewIndex := rf.log.LastIndex()
		if rf.commitIndex > lastNewIndex {
			rf.commitIndex = lastNewIndex
		}
		DPrintf("[S%v]: follower commitIndex change %v", rf.me, rf.commitIndex)
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) sendAppendRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendRequest", args, reply)
	return ok
}

func (rf *Raft) commitL() {
	start := rf.commitIndex + 1
	if start < rf.log.FirstIndex() {
		start = rf.log.FirstIndex()
	}

	for i := start; i <= rf.log.LastIndex(); i++ {
		if rf.currentTerm != rf.log.GetAt(i).Term {
			continue
		}
		n := 1
		for j, _ := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i {
				n += 1
			}
			if n > len(rf.peers)/2 {
				rf.commitIndex = i
				DPrintf("[S%v]: commit %v", rf.me, rf.commitIndex)
				break
			}
		}
	}
	rf.applyCond.Broadcast()
}

func (rf *Raft) processConflictTermL(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) int {

	if reply.ConflictTerm == -1 {
		// case 1:
		// me:     4
		// leader: 4 6 6 6
		return reply.ConflictIndex
	}
	// check whether conflict term exist in leader's log
	index := args.PrevLogIndex

	if index > rf.log.LastIndex() {
		index = rf.log.LastIndex()
	}

	for index > rf.log.FirstIndex() && rf.log.GetAt(index).Term > reply.ConflictTerm {
		index--
	}
	if index <= rf.log.FirstIndex() {
		// case 3:
		// me:     5 5 5
		// leader: 4 6 6 6
		return index
	}
	if rf.log.GetAt(index).Term == reply.ConflictTerm {
		// case 2:
		// me:     4 4 4
		// leader: 4 6 6 6
		return index + 1
	} else {
		// case 4:
		// me:     4 5 5
		// leader: 4 6 6 6
		return reply.ConflictIndex
	}
}

func (rf *Raft) handleAppendReplyL(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.newTermL(reply.Term)
	} else {
		if reply.Success {
			next := args.PrevLogIndex + len(args.Entries) + 1
			match := args.PrevLogIndex + len(args.Entries)
			if next > rf.nextIndex[server] {
				rf.nextIndex[server] = next
			}
			if match > rf.matchIndex[server] {
				rf.matchIndex[server] = match
			}
			DPrintf("[S%v]: AppendRequest append [S%v] success match %v next %v reply %v\n", rf.me, server, rf.matchIndex[server], rf.nextIndex[server], reply)
		} else {
			//DPrintf("[S%v]: last: %v, next: %v\n", rf.me, args.PrevLogIndex, rf.nextIndex[server])

			rf.nextIndex[server] = rf.processConflictTermL(server, args, reply)
			DPrintf("[S%v]: AppendRequest append [S%v] fail, back to %v, reply %v\n", rf.me, server, rf.nextIndex[server], reply)
		}
	}
	// commit
	rf.commitL()
}

func (rf *Raft) sendAppend(server int) {

	rf.mu.Lock()
	// must check the role there, because becoming follower and sendAppend is concurrent
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1
	if prevLogIndex > rf.log.LastIndex() {
		prevLogIndex = rf.log.LastIndex()
	}

	if prevLogIndex < rf.log.FirstIndex() { // it's time to send a snapshot

		args := rf.makeSnapshotInstallArgs()
		rf.mu.Unlock()

		DPrintf("[S%v]: SnapshotInstallRequest send to [S%v] with %v", rf.me, server, args)
		var reply InstallSnapshotReply
		ok := rf.sendSnapshotRequest(server, args, &reply)
		if ok {
			rf.mu.Lock()
			rf.handleSnapshotReplyL(server, args, &reply)
			rf.mu.Unlock()
		}
	} else {
		args := rf.makeAppendEntriesArgs(prevLogIndex)
		rf.mu.Unlock()

		DPrintf("[S%v]: AppendRequest send to [S%v] with %v", rf.me, server, args)
		var reply AppendEntriesReply
		ok := rf.sendAppendRequest(server, args, &reply)
		if ok {
			rf.mu.Lock()
			rf.handleAppendReplyL(server, args, &reply)
			rf.mu.Unlock()
		}
	}
}

// heartbeat and append entries
func (rf *Raft) sendAppendsL(isHeartBeat bool) {

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		if isHeartBeat {
			go rf.sendAppend(i)
		} else {
			rf.replicatorCond[i].Broadcast()
		}
	}
}

func (rf *Raft) makeAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {

	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		prevLogIndex,
		rf.log.GetAt(prevLogIndex).Term,
		make([]Entry, rf.log.LastIndex()-prevLogIndex),
		rf.commitIndex,
	}
	copy(args.Entries, rf.log.Splice(prevLogIndex+1))
	return &args
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()

	for rf.killed() == false {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.sendAppend(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("[S%v]: matchIndex[%v]=%v rf.log %v index %v", rf.me, peer, rf.matchIndex[peer], rf.log, rf.log.LastIndex())
	return rf.role == Leader && rf.matchIndex[peer] < rf.log.LastIndex()
}
