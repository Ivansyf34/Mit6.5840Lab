package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	//"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		//log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId 	int64
	Seq      	int64
	Do			string
	Key			string
	Value		string
}

type KVServer struct {
	mu     	 	sync.Mutex
	me     	 	int
	rf      	*raft.Raft
	applyCh 	chan raft.ApplyMsg
	dead    	int32 // set by Kill()

	maxraftstate int // snapshot if //log grows this big

	// Your definitions here.
	kvMap		map[string]string
	seqMap		map[int64]int64
	replyMap	map[IndexAndTerm]chan OpAppendReply
	lastApplied int
}

type IndexAndTerm struct {
    Index int
    Term  int
}

func (kv *KVServer) getChan(indexandterm IndexAndTerm) chan OpAppendReply{
	//log.Printf("[getChan] begin!")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exist := kv.replyMap[indexandterm]
	if !exist{
		kv.replyMap[indexandterm] = make(chan OpAppendReply, 1)
	}
	//log.Printf("[getChan] end!")
    
	return kv.replyMap[indexandterm]
}

func (kv *KVServer) isDuplicateOp(op *Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastSeq, ok := kv.seqMap[op.ClientId]
	if !ok{
		return false
	}
	return lastSeq >= op.Seq
}

func (kv *KVServer) Op(args *OpAppendArgs, reply *OpAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	////log.Printf("[Op] %d找到leader啦", kv.me)
	//log.Printf("[Op] leader %d 收到Op", kv.me)

	op := Op{ClientId: args.ClientId, Seq: args.Seq, Do: args.Op, Key: args.Key, Value: args.Value}
	if(args.Op != "Get" && kv.isDuplicateOp(&op)){
		reply.Err = OK
		return
	}

	index, term, _ := kv.rf.Start(op)
	//log.Printf("[Op] %d, index = %d, term = %d, op = %s, key =%s, value = %s", kv.me, index, term,args.Op, args.Key, args.Value)

	it := IndexAndTerm{index, term}
    ch := kv.getChan(it)
    defer func() {
        kv.mu.Lock()
        delete(kv.replyMap, it)
        kv.mu.Unlock()
    }()

	// 设置超时ticker
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()

    select {
    case response := <-ch:
		//log.Printf("[Op] %d success, term:%d, commandindex:%d", kv.me, it.Term, it.Index)
		reply.Err = response.Err
		reply.Value = response.Value
        return
    case <-timer.C:
		//log.Printf("[Op] %d timeout, term:%d, commandindex:%d", kv.me, it.Term, it.Index)
		reply.Err = ErrWrongLeader
        return
    }

}

func (kv *KVServer) applyOp() {
	for !kv.killed() {
		select{
		case msg := <-kv.applyCh:
			if msg.CommandValid{
				if msg.CommandIndex <= kv.lastApplied {
					continue
				}
				//log.Printf("[applyOp] %d,commandindex = %d", kv.me, msg.CommandIndex)
				var cmd Op = msg.Command.(Op)
				op := cmd.Do
				//kv.lastApplied = msg.CommandIndex
				var response OpAppendReply
				if op != "Get" && kv.isDuplicateOp(&Op{ClientId: cmd.ClientId, Seq: cmd.Seq}) {
					response = OpAppendReply{OK, ""}
				} else {
					kv.mu.Lock()
					kv.seqMap[cmd.ClientId] = cmd.Seq
					switch op {
					case "Put":
						kv.kvMap[cmd.Key] = cmd.Value
						response = OpAppendReply{OK, ""}
					case "Append":
						kv.kvMap[cmd.Key] += cmd.Value
						response = OpAppendReply{OK, ""}
					case "Get":
						if value, ok := kv.kvMap[cmd.Key]; ok {
							response = OpAppendReply{OK, value}
						} else {
							response = OpAppendReply{ErrNoKey, ""}
						}
					}
					kv.mu.Unlock()
				}

				// 超过阈值, 让rf进行snapshot
				if kv.maxraftstate != -1 && kv.maxraftstate < kv.rf.GetRaftStateSize(){
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}

				if currentTerm, isLeader := kv.rf.GetState(); isLeader {
					//log.Printf("[applyOp] %d reply, response:%v, term:%d, commandindex:%d", kv.me, response.Err,currentTerm, msg.CommandIndex)
					ch := kv.getChan(IndexAndTerm{msg.CommandIndex, currentTerm})
					ch <- response
					//log.Printf("[applyOp] %d reply, end", kv.me)
				}
			}else if msg.SnapshotValid{
				kv.mu.Lock()
				kv.DecodeSnapShot(msg.Snapshot)
				kv.lastApplied = msg.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvMap map[string]string
	var seqMap map[int64]int64
	//var lastApplied int

	if d.Decode(&kvMap) == nil && d.Decode(&seqMap) == nil {
		kv.kvMap = kvMap
		kv.seqMap = seqMap
		//kv.lastApplied = lastApplied
	} else {
		//log.Printf("S%d Failed to decode snapshot", kv.me)
	}
}

func (kv *KVServer) PersistSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.seqMap)
	//e.Encode(kv.lastApplied)
	data := w.Bytes()
	return data
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its //log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.seqMap = make(map[int64]int64)
	kv.replyMap = make(map[IndexAndTerm]chan OpAppendReply)
	kv.lastApplied = 0

	// 因为可能会crash重连
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	go kv.applyOp()
	return kv
}
