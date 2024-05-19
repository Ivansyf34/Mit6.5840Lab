package raft

import "time"
//import "log"
//import "fmt"

type InstallSnapshotArgs struct {
	Term             int    // leader's term
	LeaderId         int    // so follower can redirect clients
	LastIncludeIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludeTerm  int    // term of lastIncludedIndex
	Data             []byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	// Your data here (2A).
	Term 			int
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool{
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

func (rf *Raft) leaderSendSnapShot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludeIndex,
		rf.lastIncludeTerm,
		rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()
	//log.Printf("[leaderSendSnapShot] %d向%d发送snapshot, lastlincludeindex = %d, lastincludeterm = %d",rf.me, server, rf.lastIncludeIndex, rf.lastIncludeTerm)
	//fmt.Printf("[leaderSendSnapShot] %d向%d发送snapshot, lastlincludeindex = %d, lastincludeterm = %d\n",rf.me, server, rf.lastIncludeIndex, rf.lastIncludeTerm)

	res := rf.sendSnapshot(server, &args, &reply)

	if res {
		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		// 如果返回的term比自己大说明自身数据已经不合适了
		if reply.Term > rf.currentTerm {
			rf.role = Follower
			rf.votedFor = -1
			rf.havevoted = 0
			rf.persist()
			rf.electionTimer = time.Now()
			rf.mu.Unlock()
			return
		}
		// reset matchIndex nextIndex
		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	//log.Printf("[InstallSnapShot] %d 收到,args.lasticx= %d, mylasticx = %d", rf.me, args.LastIncludeIndex, rf.lastIncludeIndex)
	//fmt.Printf("[InstallSnapShot] %d 收到,args.lasticx= %d, mylasticx = %d\n", rf.me, args.LastIncludeIndex, rf.lastIncludeIndex)
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}else{
		rf.currentTerm = args.Term
		reply.Term = args.Term

		rf.role = Follower
		rf.votedFor = -1
		rf.havevoted = 0
		rf.persist()
		rf.electionTimer = time.Now()
	}

	/* if rf.commitIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		return
	} */

	if rf.lastIncludeIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}

	index := args.LastIncludeIndex
	if index > rf.getLastLogIndex(){
		rf.log = make([]LogEntry, 0)
	}else{
		rf.log = rf.log[rf.getLogIndex(index + 1) - 1:]
	}
	
	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.lastIncludeIndex = args.LastIncludeIndex
	//fmt.Printf("[InstallSnapShot] %d ,lastIncludeIndex = %d\n",rf.me, rf.lastIncludeIndex )
	rf.persist()
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.Save(rf.persister.ReadRaftState(), args.Data)

	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}
	rf.mu.Unlock()

	rf.applyCh <- msg

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	// index比已经snap的index小，重复了
	if index <= rf.lastIncludeIndex{
		return
	}
	// index大于提交的index
	if rf.commitIndex < index{
		return
	}
	// index要确保小于lastlogindex
	if(index > rf.getLastLogIndex()){
		index = rf.getLastLogIndex()
	}
	//log.Printf("[Snapshot] %d do snapshot, index = %d", rf.me, index)
	//fmt.Printf("[Snapshot] %d do snapshot, index = %d\n", rf.me, index)

	// 更新内容
	rf.lastIncludeTerm = rf.getLogTerm(index)
	rf.log = rf.log[rf.getLogIndex(index):]
	rf.lastIncludeIndex = index

	rf.persist()
	//log.Printf("[Snapshot] %d log len = %d", rf.me, len(rf.log))
	//fmt.Printf("[Snapshot] %d log len = %d, lastIncludeIndex = %d\n", rf.me, len(rf.log),rf.lastIncludeIndex)
	
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.Save(rf.persister.ReadRaftState(), snapshot)
}