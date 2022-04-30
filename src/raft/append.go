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
	Term                   int
	ConflictTerm           int
	ConflictTermFirstIndex int
	LastIndex              int
	Success                bool
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

	rf.setElectionTimeoutL()

	if args.Term > rf.currentTerm {
		// All Servers: 1
		rf.newTermL(args.Term)
	}

	if args.Term < rf.currentTerm {
		// AppendEntries Receiver implementation: 1
		reply.Success = false
	} else {
		reply.LastIndex = rf.log.LastIndex()
		if rf.log.LastIndex() < args.PrevLogIndex {
			// AppendEntries Receiver implementation: 2
			reply.Success = false
			reply.ConflictTerm = -1
		} else {
			if rf.log.GetAt(args.PrevLogIndex).Term != args.PrevLogTerm {
				// AppendEntries Receiver implementation: 3
				reply.ConflictTerm = rf.log.GetAt(args.PrevLogIndex).Term
				// get first index of ConflictTerm
				reply.ConflictTermFirstIndex = args.PrevLogIndex
				for reply.ConflictTermFirstIndex-1 >= rf.log.FirstIndex() &&
					rf.log.GetAt(reply.ConflictTermFirstIndex-1).Term == reply.ConflictTerm {
					reply.ConflictTermFirstIndex -= 1
				}
				rf.log.EraseAfter(args.PrevLogIndex)
				reply.Success = false
			} else {

				for i, e := range args.Entries {
					// AppendEntries Receiver implementation: 4
					if args.PrevLogIndex+1+i <= rf.log.LastIndex() {
						// delete old log and avoid duplicate log
						// Case:
						// Me:     1 3 2 2
						// Leader: 1 3
						if rf.log.GetAt(args.PrevLogIndex + i + 1).isEqual(e) {
							// duplicate log
							continue
						} else {
							// old log should be deleted
							rf.log.EraseAfter(args.PrevLogIndex + i + 1)
						}
					}
					rf.log.Append(e)
					// DPrintf("[S%v]: %v from [S%v] appended, now %v\n", rf.me, e, args.LeaderId, rf.log)
				}
				// AppendEntries Receiver implementation: 5
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = args.LeaderCommit
					lastNewIndex := args.PrevLogIndex + 1 + len(args.Entries) - 1
					if rf.commitIndex > lastNewIndex {
						rf.commitIndex = lastNewIndex
					}
					rf.applyCond.Broadcast()
				}
				reply.Success = true
			}

		}
	}

	reply.Term = rf.currentTerm
	rf.persist()
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
		return reply.LastIndex + 1
	}
	// check whether conflict term exist in leader's log
	index := args.PrevLogIndex

	if index > rf.log.LastIndex() {
		DPrintf("[S%v]: very bad\n", rf.me)
		index = rf.log.LastIndex()
	}

	for index >= rf.log.FirstIndex() && rf.log.GetAt(index).Term > reply.ConflictTerm {
		index--
	}
	if rf.log.GetAt(index).Term == reply.ConflictTerm {
		// case 2:
		// me:     4 4 4
		// leader: 4 6 6 6
		return index + 1
	} else {
		if index < rf.log.FirstIndex() {
			// case 3:
			// me:     5 5 5
			// leader: 4 6 6 6
			return index
		} else {
			// case 4:
			// me:     4 5 5
			// leader: 4 6 6 6
			return reply.ConflictTermFirstIndex
		}
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
			DPrintf("[S%v]: last: %v, next: %v\n", rf.me, args.PrevLogIndex, rf.nextIndex[server])

			rf.nextIndex[server] = rf.processConflictTermL(server, args, reply)

			if rf.nextIndex[server] <= rf.log.FirstIndex() {
				// send snapshot
			}
			DPrintf("[S%v]: AppendRequest append [S%v] fail, back to %v, reply %v\n", rf.me, server, rf.nextIndex[server], reply)
		}
	}
	// commit
	rf.commitL()
}

func (rf *Raft) sendAppendL(server int) {
	next := rf.nextIndex[server]

	if next > rf.log.LastIndex()+1 {
		next = rf.log.LastIndex()
	}

	if next-1 < rf.log.FirstIndex() {
		next = rf.log.FirstIndex() + 1
	}

	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		next - 1,
		rf.log.GetAt(next - 1).Term,
		make([]Entry, rf.log.LastIndex()-next+1),
		rf.commitIndex,
	}
	copy(args.Entries, rf.log.Splice(next))

	go func() {
		DPrintf("[S%v]: AppendRequest send to [S%v] with %v", rf.me, server, &args)
		var reply AppendEntriesReply
		ok := rf.sendAppendRequest(server, &args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.handleAppendReplyL(server, &args, &reply)
		}
	}()

}

// heartbeat and append entries
func (rf *Raft) sendAppendsL() {

	for i, _ := range rf.peers {
		if i != rf.me {
			rf.sendAppendL(i)
		}
	}
}
