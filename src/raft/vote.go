package raft

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	tickInterval    = 50 // ms
	electionTimeout = 1000
)

type Role int

const (
	Leader Role = iota + 1
	Follower
	Candidate
)

func (r *Role) String() string {
	var str string
	switch *r {
	case Follower:
		str = "Follower"
	case Leader:
		str = "Leader"
	case Candidate:
		str = "Candidate"
	}
	return str
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (rva *RequestVoteArgs) String() string {
	return fmt.Sprintf("Term %v CandidateId %v LastLogIndex %v LastLogTerm%v",
		rva.Term, rva.CandidateId, rva.LastLogIndex, rva.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rvr *RequestVoteReply) String() string {
	return fmt.Sprintf("Term %v VoteGranted %v", rvr.Term, rvr.VoteGranted)
}

// Respond to RPCs from candidates and leaders
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[S%v]: RequestVote receive from [S%v] with %v", rf.me, args.CandidateId, args)
	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
	}

	lastIndex := rf.log.LastIndex()
	lastTerm := rf.log.GetAt(lastIndex).Term
	isNew := (lastTerm < args.LastLogTerm) || (lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isNew { // 变成leader之后就不会再给同一个term中竞争的candidate投票了
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		// follower rule 5.2
		rf.setElectionTimeoutL()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) becomeLeader() {
	DPrintf("[S%v]: become a leader log len %v\n", rf.me, len(rf.log.Entries))
	rf.role = Leader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.LastIndex() + 1
	}
}

func (rf *Raft) requestVote(args RequestVoteArgs, server int) {
	var reply RequestVoteReply
	DPrintf("[S%v]: RequestVote send to [S%v] with %v", rf.me, server, &args)
	ok := rf.sendRequestVote(server, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		DPrintf("[S%v]: RequestVote reply by [S%v] with %v\n", rf.me, server, reply)

		// If AppendEntries RPC received from new leader: convert to follower
		if reply.Term > args.Term {
			rf.newTermL(reply.Term)
		}

		if reply.VoteGranted {
			rf.votes += 1
			// If votes received from majority of servers: become leader
			if rf.votes > len(rf.peers)/2 {
				if rf.role == Candidate { // 参选者有可能已经发现别的leader，并变成follower了
					rf.becomeLeader()
					// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
					rf.sendAppendsL() // 成为leader后立即发送心跳
				}
			}
		}
	}
}

func (rf *Raft) startElectionL() {
	rf.currentTerm += 1
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.persist()

	DPrintf("[S%v]: attempting an election at term %v\n", rf.me, rf.currentTerm)

	rf.votes = 1

	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.log.LastIndex(),
		rf.log.GetAt(rf.log.LastIndex()).Term}

	// 向除自己外的其他peers发送请求
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.requestVote(args, i)
		}
	}
}

func (rf *Raft) newTermL(term int) {
	DPrintf("[S%v]: new Term %v, become Follower\n", rf.me, term)
	rf.role = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("[S%v]: tick role %v\n", rf.me, rf.role)

	//  repeat during idle periods to prevent election timeouts
	if rf.role == Leader {
		rf.setElectionTimeoutL() // leader不会超时
		rf.sendAppendsL()
	}

	// If election timeout elapses: start new election
	if time.Now().After(rf.electionTimeout) {
		rf.setElectionTimeoutL() // 进入选举，重设超时，如果在这个时间内没有选举成功就开始新一轮选举
		rf.startElectionL()
	}
}

func (rf *Raft) setElectionTimeoutL() {
	ms := electionTimeout + rand.Int63()%300
	rf.electionTimeout = time.Now().Add(time.Millisecond * time.Duration(ms))
}

func (rf *Raft) startTick() {
	rf.setElectionTimeoutL()
	for rf.killed() == false {
		rf.tick()
		time.Sleep(time.Millisecond * tickInterval)
	}
}
