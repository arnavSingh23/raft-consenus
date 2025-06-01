package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cs351/labrpc"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // This peer's index into peers[]
	dead  int32               // Set by Kill()

	// all from paper, from my understanding we add all of what is missing
	currentTerm   int         // latest term server has seen
	votedFor      int         // candidateId that recieved a vote in the current term
	log           []Log       // log entries
	commitIndex   int         // index of highest log entry that we know has been commited
	lastApplied   int         // index of highest log entry applied to the SM
	nextIndex     []int       // index of next log entry to send to each server
	matchIndex    []int       // highest index of log entry known to be replicate on each sever
	state         int         // 0 for follower, 1 for candidate, 2 for leader
	electionTimer *time.Timer // Timer for election timeout
}

type Log struct {
	Term    int
	Command interface{} // in order for the new command to be appended to the leader's log we extend this struct
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == 2

	return term, isleader
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate term (that it has)
	CandidateId  int // id of the candidate wanting a vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // currentTerm for the candidate server (self updates/increment)
	VoteGranted bool // boolean value to describe if a vote was casted to it or not
}

// figure 2 representation for the append entries struct
type AppendEntriesArgs struct {
	Term         int   // leader term
	LeaderId     int   // leader id for client usage
	PrevLogIndex int   // literally the log entry right before any new ones
	PrevLogTerm  int   // term of previous log entry
	Entries      []Log // log entries to be appended
	LeaderCommit int   // leaders commit indes
}

// figure 2 representation for the append entries reply struct
type AppendEntriesReply struct {
	Term          int  // current term for the leader to update
	Success       bool // true if follower contained an entry matching log index and log term (previous)
	ConflictIndex int  // according to the paper to fall back properly in the face of conflict we must log it
	ConflictTerm  int  // same for its term as well
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// get access to lock for thread safety as always
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if the candidiate server term is less then the term, automatically reject (unfit for leader) basically outdated
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if the candidate server term is greater than the term, update our term and become follower server
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1 // consistent with the parameters set in the figure 2 and struct, reset vote
		rf.state = 0     // now follower
	}

	// check for how recently we have voted to ensure we are not voting twice or not at all in a term
	// captures our last log index and term of log
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	// here I think is an intelligent way to check if the log is up to date
	upToDate := (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	// fair to assume could fail tests but for now this boolean logic makes sense
	// bascically if candidates last log term is > than ours, or if terms are equal candidates log is as large as ours

	// this is where votes are being granted of course, follows if above condition holds and if we have not voted yet
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	// send a response with our current term regardless of any of the code above to nake sure "state" is maintained globally (updated)
	// state could be wrong word but I think I understand what I am saying
	reply.Term = rf.currentTerm
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
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
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock() // lock the raft such that there is safe reads and updates upon shared resources
	defer rf.mu.Unlock()

	// check if the current server believes that it is the leader
	if rf.state != 2 { // checks the inverse if it is not the leader it is easier to initialize the vars above
		return -1, rf.currentTerm, false // instantiate the current term and set isLeader to false
	}

	// if the above condition is not triggered we can continue with the appending process to the log
	newEntry := Log{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newEntry) // this will finally append the new log entry to this server's log

	// according to the tests if I am understanding correctly, the testing code expects first commited
	// command to show up at index 1 so we gotta make sure it does that
	index = len(rf.log) - 1
	term = rf.currentTerm
	isLeader = true

	// we must now call the sendAppendEntries RPC in order to facilitate the log replication
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i)
		}
	}

	return index, term, isLeader
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// random election timeout, I believe this needs to be added even if not in skeleton but here it is
// again in line with the paper's rec for timeout duration
func randomElectionTimeout() time.Duration {
	return time.Duration(350+rand.Intn(200)) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// Basically doing what is mentioned in the comments
		timeout := randomElectionTimeout()
		time.Sleep(timeout)

		rf.mu.Lock()
		if rf.state == 2 { // start election if not leader pretty explanatory
			rf.mu.Unlock()
			continue
		}

		// start the election and become candidate, increment term, and vote for yourself (server)
		rf.state = 1 // candidate intitation
		rf.currentTerm++
		currentTerm := rf.currentTerm
		rf.votedFor = rf.me
		rf.mu.Unlock()

		// vote for yourself
		voteCount := 1
		var mu sync.Mutex
		var wg sync.WaitGroup

		// Send RequestVote RPCs in a concurrent way to all other servers
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			wg.Add(1)
			go func(server int) {
				defer wg.Done()
				args := RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm:  0,
				}
				if args.LastLogIndex >= 0 {
					args.LastLogTerm = rf.log[args.LastLogIndex].Term
				}

				var reply RequestVoteReply
				if rf.sendRequestVote(server, &args, &reply) {
					// obtain reply and update the state of the server
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						// if server term is outdated demote it
						rf.currentTerm = reply.Term
						rf.state = 0 // demoted to follower, which is inline with behavior
						rf.votedFor = -1
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()

					// increment vote if it is valid or counted
					if reply.VoteGranted {
						mu.Lock()
						voteCount++
						mu.Unlock()
					}
				}
			}(i)
		}

		// Wait for all RPCs to complete NOTE: this always follows concurrent calls
		wg.Wait()

		// check if votes are valid and server is candidate, very procdedural cool!
		rf.mu.Lock()
		if rf.state == 1 && voteCount > len(rf.peers)/2 {
			// above code checks if server won election if so become leader
			rf.state = 2
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log)
				if i == rf.me {
					rf.matchIndex[i] = len(rf.log) - 1
				} else {
					rf.matchIndex[i] = 0
				}
			}
			// start sending the heartbeats we added for leader nodes immediatly
			go rf.heartbeatSender()
		}
		// Otherwise, remain candidate or revert to follower.
		rf.mu.Unlock()
	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (3A, 3B).
	rf.currentTerm = 0                     // initially set to term 0
	rf.votedFor = -1                       // Again to note that no candidate has recieved a vote
	rf.log = make([]Log, 1)                // insantiate an empty log in the beginning
	rf.commitIndex = 0                     // no log entry commited at the moment
	rf.lastApplied = 0                     // no log entry has been applied yet
	rf.state = 0                           // intial role to follower
	rf.nextIndex = make([]int, len(peers)) // when a server becomes leader these two fields will be populated
	rf.matchIndex = make([]int, len(peers))

	// start ticker goroutine to start elections.
	go rf.ticker()
	go rf.applier(applyCh)
	return rf
}

// RPC handler for AppendEntries idea is that it will handle the heartbeat process and log replication from LEADER
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// we will reject (return false) if the term is less than the current term
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// if the leader has a fresher term update and become follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = 0 // set state to become follower
	}

	// Reset election timer whenever a valid action is complete
	if rf.electionTimer != nil {
		rf.electionTimer.Reset(randomElectionTimeout())
	}

	// Here we will follow the paper as explicitly as we can to make sure there isa matching log entry at PrevLogIndex
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.ConflictIndex = len(rf.log)
		return
	}
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		conflictIndex := args.PrevLogIndex
		for conflictIndex > 0 && rf.log[conflictIndex-1].Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		return

	}

	// append the new entries and if there are conflicts you overwrite them
	index := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		targetIndex := index + i
		if targetIndex >= len(rf.log) {
			// iterate through the remaining entries and appened them
			for j := i; j < len(args.Entries); j++ {
				rf.log = append(rf.log, args.Entries[j])
			}
			break
		}
		if rf.log[targetIndex].Term != entry.Term {
			// remove the conflict
			rf.log = rf.log[:targetIndex]
			// same idea iterate and add
			for j := i; j < len(args.Entries); j++ {
				rf.log = append(rf.log, args.Entries[j])
			}
			break
		}
	}

	// keep the commitindex up to date
	if args.LeaderCommit > rf.commitIndex {
		lastIndex := len(rf.log) - 1
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
	}

	reply.Success = true
}

// function that will allow for the leader to facilitate the exchange between itself and follower nodes
func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()

	// not leader short circuit!
	if rf.state != 2 {
		rf.mu.Unlock()
		return
	}

	// get the indexes required and the slice [] of the log that will be sent
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
		prevLogTerm = rf.log[prevLogIndex].Term
	}

	entries := make([]Log, len(rf.log[rf.nextIndex[server]:])) // call our make()
	copy(entries, rf.log[rf.nextIndex[server]:])

	args := AppendEntriesArgs{ // set the args required by our struct!
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	var reply AppendEntriesReply
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply) // the rpc calls NOTE the string name

	if ok { // if the call is made NOTE think like HTTP status codes in the RPC context
		rf.mu.Lock() // obtain locks needed for concurrency control, argubably the most important context I have seen it in
		defer rf.mu.Unlock()

		// check if term needs to be updated
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = 0 // become follower
			rf.votedFor = -1
			return
		}

		if reply.Success {
			// if there is a success update the match index and the next index
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			// try to commit new entries and update the commit index
			newCommitIndex := rf.commitIndex
			for i := rf.commitIndex + 1; i < len(rf.log); i++ {
				if rf.log[i].Term != rf.currentTerm {
					continue
				}

				count := 1 // conditional check for this update
				for j := range rf.peers {
					if rf.matchIndex[j] >= i {
						count++
					}
				}

				if count > len(rf.peers)/2 {
					newCommitIndex = i
				}
			}

			if newCommitIndex != rf.commitIndex {
				rf.commitIndex = newCommitIndex
			}

		} else {
			// if there is a conflict fall back and try the earlier
			if reply.ConflictTerm != -1 {
				// i think my interpreation of the functionality makes sense if we just skip the term that conflicts as we see below
				conflictIndex := -1
				for i := len(rf.log) - 1; i >= 1; i-- {
					if rf.log[i].Term == reply.ConflictTerm {
						conflictIndex = i
					}
				}
				// any matching term in our own log, use the first occurence of it
				if conflictIndex >= 0 {
					rf.nextIndex[server] = conflictIndex
				} else {
					rf.nextIndex[server] = reply.ConflictIndex
				}
			} else {
				// fallback to the follower's length so we can try sending earlier entries.
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}
	}
}

func (rf *Raft) heartbeatSender() {
	// essentially a forever loop until the server is noted as dead
	for rf.killed() == false {
		rf.mu.Lock()
		// only leaders send heartbeats so if not a leader we must exit the routine
		if rf.state != 2 {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// send the append entries rpc to other nodes
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				// this will continually reset peer nodes elections
				go rf.sendAppendEntries(i)
			}
		}
		// raft paper reccomendation for the sleep time
		time.Sleep(100 * time.Millisecond)
	}
}

// routine in order to apply commited log entries to the state machine
func (rf *Raft) applier(applyCh chan ApplyMsg) {
	for rf.killed() == false { // continue as long as server is not dead
		rf.mu.Lock()
		// apply all entries that are committed that are reamining to be applied
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{ // use the struct to format the data as we have been provided
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			// lots of issues here unless you unlock to avoid long blocking periods
			rf.mu.Unlock()
			applyCh <- msg // message is sent through the conduit
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		// way to unbound from the cpu
		time.Sleep(10 * time.Millisecond)
	}
}
