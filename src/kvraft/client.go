package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"
//import "log"
//import "time"


type Clerk struct {
	servers 		[]*labrpc.ClientEnd
	// You will have to modify this struct.
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

func (ck *Clerk) nextSeq() int64 {
	return atomic.AddInt64(&ck.seq, 1)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientid = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	return ck.Op(OpAppendArgs{Key: key, Value: "", Op: "Get"})
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string) {
	ck.Op(OpAppendArgs{Key: key, Value: value, Op: "Put"})
}
func (ck *Clerk) Append(key string, value string) {
	ck.Op(OpAppendArgs{Key: key, Value: value, Op: "Append"})
}

func (ck *Clerk) Op(args OpAppendArgs) string{
	args.ClientId = ck.clientid
	args.Seq = ck.nextSeq()
	for {
		response := OpAppendReply{}
		//log.Printf("Client[%d] SeqId[%d] leaderServer: %d op:%7s key: %s value:%s", ck.clientid, ck.seq, ck.leaderserver,args.Op, args.Key, args.Value)
		ok := ck.servers[ck.leaderserver].Call("KVServer.Op", &args, &response)
		if ok{
			if response.Err == OK{
				//log.Printf("C%d <- S%d received OK", ck.clientid, ck.leaderserver)
				return response.Value
			}else if response.Err == ErrNoKey{
				//log.Printf("C%d <- S%d received ErrNoKey", ck.clientid, ck.leaderserver)
				return ""
			}else if response.Err == ErrWrongLeader{
				ck.leaderserver = (ck.leaderserver + 1) % int64(len(ck.servers))
				//log.Printf("C%d <- S%d received ErrWrongLeader", ck.clientid, ck.leaderserver)
				continue
			}
		}
		ck.leaderserver = (ck.leaderserver + 1) % int64(len(ck.servers))
		//time.Sleep(retryTimeout)
	}
}
