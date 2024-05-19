package raft

import "time"
//import "log"
//import "fmt"

func (rf *Raft) CandidateTick(){
	rf.mu.Lock()
	rf.currentTerm++
	//log.Printf("%d,%d currentTerm = %d", rf.role, rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.persist()
	rf.electionTimer = time.Now()
	rf.havevoted = 1
	rf.mu.Unlock()
	for i:=0 ; i<len(rf.peers); i++{
		rf.mu.Lock()
		if(i != rf.me && rf.role == Candidate){
			rf.mu.Unlock()
			go func(peerIndex int) {
				var args = RequestVoteArgs{}
				var reply = RequestVoteReply{}
				rf.mu.Lock()
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastLogIndex = rf.getLastLogIndex()
				args.LastLogTerm = rf.getLastLogTerm()
				//fmt.Printf("%d发送vote给%d, term = %d\n", rf.me, peerIndex,rf.currentTerm)
				//log.Printf("[CandidateTick] %d发送vote给%d, term = %d", rf.me, peerIndex,rf.currentTerm)
				rf.mu.Unlock()
				ok := rf.sendRequestVote(peerIndex, &args, &reply)
				if ok{
					rf.mu.Lock()
					if rf.role != Candidate || args.Term < rf.currentTerm {
						rf.mu.Unlock()
						return
					}
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.havevoted = 0
						rf.role = Follower
						rf.votedFor = -1
						rf.persist()
						//log.Printf("[CandidateTick] %d成为Follower", rf.me)
						//fmt.Printf("%d成为Follower\n", rf.me)
						rf.mu.Unlock()
						return
					}
					if reply.VoteGranted && rf.currentTerm == args.Term{
						rf.havevoted+=1
						if rf.role == Candidate && rf.havevoted >= (len(rf.peers)/2 + 1){
							//fmt.Printf("%d成为Leader\n", rf.me)
							//log.Printf("[CandidateTick] %d成为Leader", rf.me)
							/* rf.votedFor = -1
							rf.persist() */
							rf.role = Leader
							rf.havevoted = 0
							for i := range rf.nextIndex {
								rf.nextIndex[i] = rf.getLastLogIndex() + 1
								rf.matchIndex[i] = 0
							}
							rf.matchIndex[rf.me] = rf.getLastLogIndex()
							rf.electionTimer = time.Now()
							rf.mu.Unlock()
							rf.LeaderTick()
							return
						}
					}
					rf.mu.Unlock()
					return
				}
			}(i) 
		}else{
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) LeaderTick(){
	for i:=0 ; i<len(rf.peers); i++{
		rf.mu.Lock()
		if(i != rf.me && rf.role == Leader){
			rf.mu.Unlock()
			go func(peerIndex int){
				rf.mu.Lock()
				if rf.role != Leader {
					rf.mu.Unlock()
					return
				}
				//fmt.Printf("[LeaderTick] %d rf.nextIndex[%d] = %d, rf.lastIncludeIndex = %d\n", rf.me, peerIndex, rf.nextIndex[peerIndex], rf.lastIncludeIndex)
				if rf.nextIndex[peerIndex] - 1 < rf.lastIncludeIndex {
					rf.mu.Unlock()
					//fmt.Printf("[LeaderTick] %d 发送snap rf.lastIncludeIndex = %d\n", rf.me, rf.lastIncludeIndex)
					go rf.leaderSendSnapShot(peerIndex)
					return
				}

				var args = AppendEntriesArgs{}
				var reply = AppendEntriesReply{}
				args.Term =  rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex

				idx:= rf.nextIndex[peerIndex] - 1
				args.PrevLogIndex, args.PrevLogTerm = rf.getPrevLogInfo(peerIndex)
				
				last_idx := rf.getLogIndex(idx)
				if(last_idx < len(rf.log)){
					//log.Printf("[LeaderTick] %d发送command%d给%d, commitIndex = %d",rf.me, rf.//log[last_idx].Index, peerIndex, rf.commitIndex)
					entries := rf.log[last_idx:]
					args.Entries = make([]LogEntry, len(entries))
					copy(args.Entries, entries)
				}else{
					args.Entries = make([]LogEntry, 0)
				}
				rf.mu.Unlock()
				
				//fmt.Printf("[LeaderTick] %d 发送心跳给%d, nextIndex = %d, matchIndex = %d, commit = %d\n",rf.me, peerIndex, rf.nextIndex[peerIndex], rf.matchIndex[peerIndex], rf.commitIndex)
				ok := rf.sendAppendEntries(peerIndex, &args, &reply)
				//log.Printf("[LeaderTick] %d发送心跳给%d, nextIndex = %d, matchIndex = %d",rf.me, peerIndex, rf.nextIndex[peerIndex], rf.matchIndex[peerIndex])
				if(ok){
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.role != Leader {
						return
					}
					if reply.Term > rf.currentTerm{
						rf.votedFor = -1
						rf.electionTimer = time.Now()
						rf.currentTerm = reply.Term
						rf.persist()
						rf.role = Follower
						//fmt.Printf("%d成为Follower\n", rf.me)
						//log.Printf("[LeaderTick] %d成为Follower", rf.me)
						return
					}
					// 当前任期不等于发送请求时的任期(发送之后任期发生改变) -> 放弃该回复
					if rf.currentTerm != args.Term {
						return
					}
					//同步成功，更新下标
					if reply.Success {	
						temp_len:=len(args.Entries)
						if(temp_len > 0){
							rf.nextIndex[peerIndex] = args.PrevLogIndex + temp_len + 1	
							rf.matchIndex[peerIndex] = rf.nextIndex[peerIndex] - 1		
						}
					}

					if reply.ConflictTerm == 0{
						// 日志断层
						rf.nextIndex[peerIndex] = reply.ConflictIndex + 1
					}else if reply.ConflictTerm > 0{
						rf.nextIndex[peerIndex] = reply.ConflictIndex
						// 日志term不一致
						lastIndex := rf.getLogLen()
						for ; lastIndex > 0; lastIndex--{
							if rf.getLogTerm(lastIndex) == reply.ConflictTerm{
								break
							}
						}
						if lastIndex > reply.ConflictIndex{
							rf.nextIndex[peerIndex] = lastIndex + 1
						}
					}
					if rf.nextIndex[peerIndex] < 1 {
						rf.nextIndex[peerIndex] = 1
					}

					//log.Printf("[LeaderTick] %d更新commitindex:%d, appliedindex:%d",rf.me ,rf.commitIndex , rf.lastApplied)
					for n:=rf.getLogLen(); n > rf.commitIndex; n--{
						cnt:=1
						if(rf.getLogTerm(n) == rf.currentTerm){
							for j:=0 ; j<len(rf.peers); j++{
								if(j != rf.me && rf.matchIndex[j] >= n){
									cnt++
								}
							}
						}
						//////log.Printf("cnt = %d",cnt)
						if(cnt >= (len(rf.peers)/2 + 1)){
							if rf.commitIndex < n{
								rf.commitIndex = n
							}
							return
						}
					}
				}
			}(i)
		}else{
			rf.mu.Unlock()
		}
		
	}
}