package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// logentry structure
type LogEntry struct {
	Command interface{}
	Term    int
}

// Raft state
const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state          int32
	latestTerm     int
	votedCandidate int

	// timers changed (these modifications were made to accomodate for 2B)
	lastElectionTimerReset  int64
	lastHeartbeatTimerReset int64
	electionTimerChan       chan bool
	heartbeatTimerChan      chan bool //heartbeat timer
	heartbeatTimeout        int64
	electionTimeout         int64

	log                 []LogEntry
	nextLogIndex        []int
	matchLogIndex       []int
	commitLogIndex      int
	lastAppliedLogEntry int
	applyCh             chan ApplyMsg // channel to send commit
}

// return latestTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.latestTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int

	// index for last log entry of candidate
	LatestLogIndex int

	// term for last log entry of candidate
	LatestLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // latestTerm, for candidate to update itself
	VoteGranted bool // if candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//modified to accomodate for 2B
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.latestTerm {
		rf.convertStateTo(Follower)
		rf.latestTerm = args.Term
	}
	// if candidate's term is less than receiver's term, then receiver rejects vote
	if args.Term < rf.latestTerm || (rf.votedCandidate != -1 && rf.votedCandidate != args.CandidateID) {

		reply.Term = rf.latestTerm
		reply.VoteGranted = false
		return
	}

	// if candidate's term is greater than receiver's term, then receiver updates its term
	if rf.votedCandidate == -1 || rf.votedCandidate == args.CandidateID {

		lastLogIndex := len(rf.log) - 1
		if rf.log[lastLogIndex].Term > args.LatestLogTerm ||
			(rf.log[lastLogIndex].Term == args.LatestLogTerm && len(rf.log)-1 > lastLogIndex) {
			reply.Term = rf.latestTerm
			reply.VoteGranted = false
			return
		}

		reply.Term = rf.latestTerm
		reply.VoteGranted = true
		rf.votedCandidate = args.CandidateID

		// timer reset on vote granting to other peer
		rf.resetElectionTimer()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// func (rf *Raft) electAfterTimeout() {
// 	timeout := rand.Int63n(700) + 300
// 	curTime := time.Now().UnixNano() / 1e6

// 	rf.mu.Lock()
// 	if curTime-rf.timer > timeout {
// 		rf.startElection()
// 	}
// 	rf.mu.Unlock()
// }

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.convertStateTo(Candidate)
	nVote := 1

	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		// skip self
		if i == rf.me {
			continue
		}
		// send request vote to all other peers
		go func(id int) {

			rf.mu.Lock()
			finalLogIndex := len(rf.log) - 1
			args := RequestVoteArgs{
				Term:           rf.latestTerm,
				CandidateID:    rf.me,
				LatestLogIndex: finalLogIndex,
				LatestLogTerm:  rf.log[finalLogIndex].Term,
			}
			rf.mu.Unlock()
			var reply RequestVoteReply
			if rf.sendRequestVote(id, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.latestTerm != args.Term {
					return
				}
				// if vote granted, then increment vote count
				if reply.VoteGranted {
					nVote += 1
					// if majority vote, then become leader
					if nVote > len(rf.peers)/2 && rf.state == Candidate {
						rf.convertStateTo(Leader)

						for i := 0; i < len(rf.peers); i++ {
							rf.nextLogIndex[i] = len(rf.log)
							rf.matchLogIndex[i] = 0
						}

						rf.mu.Unlock()
						rf.broadcastHeartbeat()
						rf.mu.Lock()
					}
				} else {
					// if vote not granted, then check if term is stale and update
					if rf.latestTerm < reply.Term {
						rf.convertStateTo(Follower)
						rf.latestTerm = reply.Term
					}
				}
			} else {
				// RPC FAIL else condition
			}
		}(i)
	}
}

//
type AppendEntriesArgs struct {
	Term int

	// 2B part
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
type AppendEntriesReply struct {
	Term    int // latestTerm
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if term is stale, then reject
	if args.Term < rf.latestTerm {
		reply.Term = rf.latestTerm
		reply.Success = false

		return
	}
	rf.resetElectionTimer()
	// if term is not stale, then update term and convert to follower
	if args.Term > rf.latestTerm {
		rf.convertStateTo(Follower)
		rf.latestTerm = args.Term
	// if state is candidate, then convert to follower
	} else if rf.state == Candidate {
		rf.state = Follower

	}

	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm, then reject
		reply.Term = rf.latestTerm
		reply.Success = false
	} else {

		isMatch := true
		nextLogIndex := args.PrevLogIndex + 1
		conflictIndex := 0
		logLen := len(rf.log)
		entLen := len(args.Entries)
		for i := 0; isMatch && i < entLen; i++ {
			if ((logLen - 1) < (nextLogIndex + i)) || rf.log[nextLogIndex+i].Term != args.Entries[i].Term {
				isMatch = false
				conflictIndex = i
				break
			}
		}

		if !isMatch {
		
			rf.log = append(rf.log[:nextLogIndex+conflictIndex], args.Entries[conflictIndex:]...)
		}

		lastNewEntryIndex := args.PrevLogIndex + entLen
		if args.LeaderCommit > rf.commitLogIndex {
			rf.commitLogIndex = min(args.LeaderCommit, lastNewEntryIndex)
			go rf.applyLogEntries()
		}

		reply.Term = rf.latestTerm
		reply.Success = true
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastHeartbeat() {

	rf.mu.Lock()
	// if not leader, then return
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.resetHeartbeatTimer()
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// send heartbeat to all other peers 
		go func(id int) {

		retry:

			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.latestTerm,
				PrevLogIndex: rf.nextLogIndex[id] - 1,
				PrevLogTerm:  rf.log[rf.nextLogIndex[id]-1].Term,
				Entries:      rf.log[rf.nextLogIndex[id]:],
				LeaderCommit: rf.commitLogIndex,
			}

			rf.mu.Unlock()

			if _, isLeader := rf.GetState(); !isLeader {
				return
			}

			var reply AppendEntriesReply
			if rf.sendAppendEntries(id, &args, &reply) {
				rf.mu.Lock()

				if rf.state != Leader {

					rf.mu.Unlock()
					return
				}
				
				if rf.latestTerm != args.Term {

					rf.mu.Unlock()
					return
				}

				if reply.Success {
					// if success, then update nextLogIndex and matchLogIndex and check for majority
					rf.matchLogIndex[id] = args.PrevLogIndex + len(args.Entries)
					rf.nextLogIndex[id] = rf.matchLogIndex[id] + 1
					rf.checkMajority()
				} else {

					if reply.Term > rf.latestTerm {
						rf.convertStateTo(Follower)
						rf.latestTerm = reply.Term
						rf.mu.Unlock()
						return
					}
					rf.nextLogIndex[id] -= 1

					rf.mu.Unlock()
					goto retry
				}
				rf.mu.Unlock()
			} else {
				// RPC FAIL else condition
			}
		}(i)
	}
}

// IDHAR AA JAO
func (rf *Raft) checkMajority() {
	// check for majority and commit log entries
	for N := len(rf.log) - 1; N > rf.commitLogIndex; N-- {
		nReplicated := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchLogIndex[i] >= N && rf.log[N].Term == rf.latestTerm {
				nReplicated += 1
			}
			if nReplicated > len(rf.peers)/2 {
				rf.commitLogIndex = N
				go rf.applyLogEntries()
				break
			}
		}

	}
}

////// HELPER FUNCTIONS //////

func (rf *Raft) electionTimer() {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			timeElapsed := (time.Now().UnixNano() - rf.lastElectionTimerReset) / time.Hour.Milliseconds()
			if timeElapsed > rf.electionTimeout {
				rf.electionTimerChan <- true
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) heartbeatTimer() {
	for {
		rf.mu.Lock()
		if rf.state == Leader {
			timeElapsed := (time.Now().UnixNano() - rf.lastHeartbeatTimerReset) / time.Hour.Milliseconds()
			if timeElapsed > rf.heartbeatTimeout {

				rf.heartbeatTimerChan <- true
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) resetElectionTimer() {
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rf.heartbeatTimeout*5 + rand.Int63n(150)
	rf.lastElectionTimerReset = time.Now().UnixNano()
}
func (rf *Raft) resetHeartbeatTimer() {
	rf.lastHeartbeatTimerReset = time.Now().UnixNano()
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// conversion through state function
func (rf *Raft) convertStateTo(state int32) {
	switch state {
	case Follower:

		rf.votedCandidate = -1
		rf.state = Follower
	case Candidate:
		rf.state = Candidate
		rf.latestTerm++
		rf.votedCandidate = rf.me
		rf.resetElectionTimer()
	case Leader:
		rf.state = Leader
		rf.resetHeartbeatTimer()
	}
}

////// HELPER FUNCTIONS  END//////

func (rf *Raft) applyLogEntries() {
	// apply log entries to state machine
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastAppliedLogEntry + 1; i <= rf.commitLogIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- applyMsg
		rf.lastAppliedLogEntry += 1
	}
}

func (rf *Raft) mainLoop() {
	for !rf.killed() {
		select {
		case <-rf.electionTimerChan:
			rf.startElection()
		case <-rf.heartbeatTimerChan:
			rf.broadcastHeartbeat()
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if term, isLeader = rf.GetState(); isLeader {
		rf.mu.Lock()
		rf.log = append(rf.log, LogEntry{command, rf.latestTerm})
		rf.matchLogIndex[rf.me] = rf.nextLogIndex[rf.me] + len(rf.log) - 1
		index = len(rf.log) - 1

		rf.mu.Unlock()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.latestTerm = 0
	rf.votedCandidate = -1
	rf.heartbeatTimerChan = make(chan bool)
	rf.electionTimerChan = make(chan bool)
	rf.heartbeatTimeout = 100 // ms
	rf.resetElectionTimer()
	// 
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.nextLogIndex = make([]int, len(rf.peers))
	rf.matchLogIndex = make([]int, len(rf.peers))
	rf.commitLogIndex = 0
	rf.lastAppliedLogEntry = 0
	rf.applyCh = applyCh

	//fmt.Printf("Starting raft %d\n", me)
	go rf.mainLoop()
	go rf.electionTimer()
	go rf.heartbeatTimer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
