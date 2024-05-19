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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"log"
	"os"
	"fmt"
	"bytes"
	"6.5840/labgob"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{} //日志记录的命令(用于应用服务的命令)
	Index   int         //该日志的索引
	Term    int         //该日志被接收的时候的Leader任期
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm 	int
	votedFor    	int
	role      		int
	electionTimer   time.Time
	havevoted		int

	log				[]LogEntry
	commitIndex 	int				// 当前log中的最高索引(从0开始,递增)
	lastApplied		int				// 当前被用到状态机中的日志最高索引(从0开始,递增)
	nextIndex		[]int  			// 发送给每台服务器的下一条日志目录索引(初始值为leader的commitIndex + 1)
	matchIndex		[]int			// 每台服务器已知的已被复制的最高日志条目索引

	applyCh			chan ApplyMsg	// 存储machine state

	lastIncludeIndex int 			// 快照中包含的最后日志条目的索引值
	lastIncludeTerm  int			// 快照中包含的最后日志条目的任期号
}


const (
	Follower          int = 0
	Candidate         int = 1
	Leader            int = 2

	APPLIEDSLEEP 	  int = 20

	// 随机生成投票过期时间范围: MoreVoteTime+MinVoteTime ~ MinVoteTime
	MoreVoteTime 	  int = 120
	MinVoteTime       int = 80
	HeartbeatSleep    int = 35
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.role == 2
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	//////////log.Printf("%d persist, log:%d",rf.me, len(rf.log))
	w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
	if e.Encode(rf.log) != nil ||
        e.Encode(rf.currentTerm) != nil ||
        e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.lastIncludeIndex) != nil ||
		e.Encode(rf.lastIncludeTerm) != nil{
        ////////log.Printf("S%d encode fail", rf.me)
    }
	raft_data := w.Bytes()

	rf.persister.Save(raft_data, rf.persister.ReadSnapshot())
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	/* if snap_data == nil || len(snap_data) < 1 { // bootstrap without any state?
		return
	} */
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log_temp []LogEntry
	var currentTerm, votedFor int
	var lastIncludeIndex, lastIncludeTerm int
	if d.Decode(&log_temp) != nil ||
        d.Decode(&currentTerm) != nil ||
        d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
        d.Decode(&lastIncludeTerm) != nil{
        //log.Printf("S%d decode fail", rf.me)
		//fmt.Printf("S%d raft decode fail\n", rf.me)
    }
	rf.log = make([]LogEntry, len(log_temp))
    copy(rf.log, log_temp)
    rf.currentTerm = currentTerm
    rf.votedFor = votedFor
	rf.lastIncludeIndex = lastIncludeIndex
	rf.lastIncludeTerm = lastIncludeTerm
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int
	LeaderId 		int
	LastLogIndex  	int
	LastLogTerm 	int     
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term 			int
	VoteGranted 	bool
}


// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
    
	//fmt.Printf("%d收到%d发来的requestvote, myterm = %d, args.term = %d\n", rf.me ,args.LeaderId, rf.currentTerm, args.Term)
	//log.Printf("[RequestVote] %d收到%d发来的requestvote, myterm = %d, args.term = %d", rf.me ,args.LeaderId, rf.currentTerm, args.Term)

    if(args.Term < rf.currentTerm){
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = rf.currentTerm

	//重置信息
	if(args.Term > rf.currentTerm){
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.havevoted = 0
		rf.persist()
	}

	//检查log
	if args.LastLogTerm < rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex()){
		reply.VoteGranted = false
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.LeaderId && args.Term == reply.Term{
		reply.VoteGranted = false
		return
	}else{
		reply.VoteGranted = true
		rf.votedFor = args.LeaderId
		rf.currentTerm = args.Term
		rf.electionTimer = time.Now()
		reply.Term = rf.currentTerm
		rf.persist()
		return
	}
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term 			int
	LeaderId 		int
	PrevLogIndex  	int
	PrevLogTerm 	int  
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term 			int
	Success 		bool
	ConflictIndex	int
	ConflictTerm	int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	//fmt.Printf("%d收到心跳,myterm = %d, term = %d, entries:%d, %d, %d,commited = %d\n",rf.me, rf.currentTerm, args.Term, len(args.Entries),args.PrevLogIndex,rf.getLastLogIndex(), rf.commitIndex)
	//log.Printf("[AppendEntries] %d收到心跳,myterm = %d, term = %d, entries:%d, %d, %d,commited = %d",rf.me, rf.currentTerm, args.Term, len(args.Entries),args.PrevLogIndex,rf.getLastLogIndex(), rf.commitIndex)
	if(args.Term < rf.currentTerm){
		rf.mu.Unlock()
		return
	}else{
		//rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.havevoted = 0
		rf.persist()
		rf.electionTimer = time.Now()
	}

	if args.PrevLogIndex < rf.commitIndex{
		reply.ConflictTerm = 0
		reply.ConflictIndex = rf.commitIndex
		rf.mu.Unlock()
		return
	}

	// 自身快照index大
	if rf.lastIncludeIndex > args.PrevLogIndex {
		reply.ConflictTerm = 0
		reply.ConflictIndex = rf.getLastLogIndex()
		rf.mu.Unlock()
		//////log.Printf("[AppendEntries] %d自身快照index大",rf.me)
		return
	}
	
	// 日志缺少
	if args.PrevLogIndex > rf.getLastLogIndex() {
        reply.ConflictTerm = 0
        reply.ConflictIndex = rf.getLastLogIndex()
		rf.mu.Unlock()
        return
    }

	// Term不一致
	cur_term := 0
	if args.PrevLogTerm != 0{
		cur_term = rf.getLogTerm(args.PrevLogIndex)
		if cur_term != args.PrevLogTerm{
			//log.Printf("lastLogTerm = %d, PrevLogTerm = %d", rf.getLastLogTerm(), args.PrevLogTerm)
			//log.Printf("%d收到心跳,lastLogTerm = %d, args.PrevLogTerm = %d, args.PrevLogIndex = %d",rf.me, rf.getLastLogTerm(), args.PrevLogTerm, args.PrevLogIndex)
			reply.Success = false
			reply.ConflictTerm = cur_term
			reply.ConflictIndex = 1
			// 0 is a dummy entry => quit in index is 1
			for index := args.PrevLogIndex; index >= 1; index-- {
				if rf.getLogTerm(index) != cur_term {
					reply.ConflictIndex = index + 1
					break
				}
			}
			rf.mu.Unlock()
			return

		}
	}

	if args.Entries != nil && len(args.Entries) != 0 {
		rf.log = rf.log[:rf.getLogIndex(args.PrevLogIndex)]
		entries := args.Entries
		for i:=0; i<len(entries); i++{
			rf.log = append(rf.log, entries[i])
			//fmt.Printf("%d增加log,command:%v, term:%d\n", rf.me, entries[i].Command, entries[i].Term)
			//log.Printf("[AppendEntries] %d增加log,command:%v, term:%d", rf.me, entries[i].Command, entries[i].Term)
		}
		reply.Success = true
		rf.persist()
	}
	if args.LeaderCommit > rf.commitIndex{
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > rf.getLastLogIndex() {
            rf.commitIndex = rf.getLastLogIndex()
        }
		rf.persist()
		rf.mu.Unlock()
		return
    }
	rf.mu.Unlock()
}

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	if(rf.role != Leader || rf.killed()){
		rf.mu.Unlock()
		return false
	} 
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	if(rf.role != Candidate || rf.killed()){
		rf.mu.Unlock()
		return false
	} 
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}



// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's //log. if this
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	
	if(rf.role != Leader){
		rf.mu.Unlock()
		return -1, -1, false
	}
	index := rf.getLogLen()
	// Your code here (2B).
	//log.Printf("[Start] %d收到client的command:%v,term:%d, index:%d",rf.me,command, rf.currentTerm, index)
	//fmt.Printf("%d收到client的command:%v,term:%d, index:%d\n",rf.me,command, rf.currentTerm, index + 1)
	logentry := LogEntry{Term: rf.currentTerm, Command: command, Index: index + 1}
	rf.log = append(rf.log, logentry)
	rf.persist()
	rf.nextIndex[rf.me] = rf.getLogLen() + 1
	defer func(){
		/* if(rf.lastIncludeIndex > 0){
			rf.mu.Unlock()
			rf.LeaderTick()
		}else{
			rf.mu.Unlock()
		} */
		rf.mu.Unlock()
		rf.LeaderTick()
	}()
	//return index + 1, currentTerm, isLeader
	return index + 1, rf.currentTerm, rf.role == Leader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 选举定时器
func (rf *Raft) ElectionTicker() {
	for !rf.killed() {
		nowTime := time.Now()
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)
		rf.mu.Lock()
		// 时间过期发起选举
		// 如果 sleep 之后，votedTimer 的时间没有被重置，还是在 nowTime 之前 -> 发起新一轮的选举
		if rf.electionTimer.Before(nowTime) && rf.role != Leader {	
			rf.role = Candidate
			rf.mu.Unlock()
			rf.CandidateTick()
		}else{
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) AppendTicker() {
	for !rf.killed() {
		time.Sleep(time.Duration(HeartbeatSleep) * time.Millisecond)
		rf.mu.Lock()
		if rf.role == Leader {
			rf.mu.Unlock()
			rf.LeaderTick()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) ApplyLog(){
	for !rf.killed(){
		time.Sleep(time.Duration(APPLIEDSLEEP) * time.Millisecond)
		rf.mu.Lock()
		if(rf.lastApplied >= rf.commitIndex){
			rf.mu.Unlock()
			continue
		}else{
			commit := rf.getLogIndex(rf.commitIndex)
			applied := rf.getLogIndex(rf.lastApplied)
			if(applied < 0){
				applied = 0
			}
			if(commit < 0){
				//log.Printf("[ApplyLog] %d applylog error, commitindex < 0", rf.me)
				//fmt.Printf("[ApplyLog] %d applylog error, commitindex < 0\n", rf.me)
				rf.mu.Unlock()
				continue
			}
			entries := make([]LogEntry, commit-applied)
			copy(entries, rf.log[applied:commit])
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()
			for _, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
				//fmt.Printf("%d的applyCh加入command:%v, Index:%d, Term:%d\n", rf.me, entry.Command, entry.Index, entry.Term)
				//log.Printf("[ApplyLog] %d的applyCh加入command:%v, Index:%d, Term:%d", rf.me, entry.Command, entry.Index, entry.Term)
			}
			
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	logFile, err := os.OpenFile("./print.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
        fmt.Println("open log file failed, err:", err)
    }
    log.SetOutput(logFile)
    log.SetFlags(log.Llongfile | log.Lmicroseconds | log.Ldate)
	rf := &Raft{ 
		peers: peers,
		persister: persister,
		me: me,
		currentTerm: 0,
		votedFor: -1,
		role: Follower,
		dead: 0,
		electionTimer: time.Now(),
		havevoted: 0,
		log: make([]LogEntry, 0),
		nextIndex: make([]int, 10),
		matchIndex: make([]int, 10),
		applyCh: applyCh,
		commitIndex: 0,
		lastApplied: 0,
		lastIncludeIndex: 0,
		lastIncludeTerm: 0,
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
	}

	// start ticker goroutine to start elections
	go rf.ElectionTicker()

	go rf.AppendTicker()

	go rf.ApplyLog()

	return rf
}
