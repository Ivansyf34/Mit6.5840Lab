package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync/atomic"
//import "log"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientid		int64
	seq				int64
	leaderserver	int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientid = nrand()
	return ck
}

func (ck *Clerk) nextSeq() int64 {
	return atomic.AddInt64(&ck.seq, 1)
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.SeqId = ck.nextSeq()
	args.ClientId = ck.clientid
	for {
		response := QueryReply{}
		//log.Printf("[QueryCall] Client[%d] SeqId[%d] leaderServer: %d op:Query", ck.clientid, ck.seq, ck.leaderserver)
		ok := ck.servers[ck.leaderserver].Call("ShardCtrler.Query", args, &response)
		//log.Printf("[QueryCall] Err = %s", response.Err)
		if ok{
			
			if response.Err == OK{
				//log.Printf("[QueryCall] return")
				return response.Config
			}else if response.Err == ErrWrongLeader{
				ck.leaderserver = (ck.leaderserver + 1) % int64(len(ck.servers))
				continue
			}
		}
		ck.leaderserver = (ck.leaderserver + 1) % int64(len(ck.servers))
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.SeqId = ck.nextSeq()
	args.ClientId = ck.clientid

	for {
		response := JoinReply{}
		//log.Printf("[JoinCall] Client[%d] SeqId[%d] leaderServer: %d op:Join", ck.clientid, ck.seq, ck.leaderserver)
		ok := ck.servers[ck.leaderserver].Call("ShardCtrler.Join", args, &response)
		if ok{
			if response.Err == OK{
				return
			}else if response.Err == ErrWrongLeader{
				ck.leaderserver = (ck.leaderserver + 1) % int64(len(ck.servers))
				continue
			}
		}
		ck.leaderserver = (ck.leaderserver + 1) % int64(len(ck.servers))
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.SeqId = ck.nextSeq()
	args.ClientId = ck.clientid

	for {
		response := LeaveReply{}
		//log.Printf("[LeaveCall] Client[%d] SeqId[%d] leaderServer: %d op:Leave", ck.clientid, ck.seq, ck.leaderserver)
		ok := ck.servers[ck.leaderserver].Call("ShardCtrler.Leave", args, &response)
		if ok{
			if response.Err == OK{
				return
			}else if response.Err == ErrWrongLeader{
				ck.leaderserver = (ck.leaderserver + 1) % int64(len(ck.servers))
				continue
			}
		}
		ck.leaderserver = (ck.leaderserver + 1) % int64(len(ck.servers))
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.SeqId = ck.nextSeq()
	args.ClientId = ck.clientid

	for {
		response := MoveReply{}
		//log.Printf("[MoveCall] Client[%d] SeqId[%d] leaderServer: %d op:Move", ck.clientid, ck.seq, ck.leaderserver)
		ok := ck.servers[ck.leaderserver].Call("ShardCtrler.Move", args, &response)
		if ok{
			if response.Err == OK{
				return
			}else if response.Err == ErrWrongLeader{
				ck.leaderserver = (ck.leaderserver + 1) % int64(len(ck.servers))
				continue
			}
		}
		ck.leaderserver = (ck.leaderserver + 1) % int64(len(ck.servers))
		time.Sleep(100 * time.Millisecond)
	}
}
