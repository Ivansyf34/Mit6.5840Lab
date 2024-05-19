package raft

import "log"
import "math/rand"
import "time"

// Debugging
const Debug = false

// 通过不同的随机种子生成不同的过期时间
func generateOverTime(server int64) int {
	randSource := rand.NewSource(time.Now().Unix() + server)
	r := rand.New(randSource)
	// Tip: r.Intn(MoreVoteTime)从 r 这个随机数生成器生成的随机整数中选择一个不超过 MoreVoteTime 的非负整数。
	return r.Intn(MoreVoteTime) + MinVoteTime
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) getLogIndex(index int) int {
	return index - rf.lastIncludeIndex
}

func (rf *Raft) getLogTerm(index int) int {
	if rf.getLogIndex(index) - 1 < 0{
		return rf.lastIncludeTerm
	}else{
		return rf.log[rf.getLogIndex(index) - 1].Term
	}
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.nextIndex[server] - 1
	lastIndex := rf.getLastLogIndex()
	if newEntryBeginIndex > lastIndex{
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.getLogTerm(newEntryBeginIndex)
}

func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludeIndex + len(rf.log)
}

func (rf *Raft) getLastLogTerm() int {
	return rf.getLogTerm(rf.getLastLogIndex())
}

func (rf *Raft) getLogLen() int {
	return rf.lastIncludeIndex + len(rf.log)
}


