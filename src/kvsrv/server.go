package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
type op struct{
	Seq		int64
	Value 	string
}

type KVServer struct {
	mu sync.Mutex
	mmap map[string]string

	// Your definitions here.
	lastClientOp map[int64]op // last operation result for each client
}

// detect duplicate operation
// return true if the operation is duplicate
func (kv *KVServer) handleDuplicateOp(args *PutAppendArgs, reply *PutAppendReply) bool {
	lastOp, ok := kv.lastClientOp[args.ClientId]
	if ok && lastOp.Seq == args.Seq {
		// same operation, return the same result
		reply.Value = lastOp.Value
		return true
	}
	return false
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock() 
	defer kv.mu.Unlock()
	value, ok := kv.mmap[args.Key]
	if !ok {
		reply.Value = ""
	} else {
		reply.Value = value
	}

	// delete last operation result for the client, release memory
	delete(kv.lastClientOp, args.ClientId)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.handleDuplicateOp(args, reply) {
		return
	}

	kv.mmap[args.Key] = args.Value

	reply.Value = ""

	// Put operation is not idempotent, so we need to store the result
	kv.lastClientOp[args.ClientId] = op{Seq: args.Seq, Value: reply.Value}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.handleDuplicateOp(args, reply) {
		return
	}

	value, ok := kv.mmap[args.Key]
	reply.Value = value
	if !ok {
		kv.mmap[args.Key] = args.Value
	} else {
		kv.mmap[args.Key] = value + args.Value
	}

	// Append operation is not idempotent, so we need to store the result
	kv.lastClientOp[args.ClientId] = op{Seq: args.Seq, Value: reply.Value}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.mmap = make(map[string]string)

	// You may need initialization code here.
	kv.lastClientOp = make(map[int64]op)

	return kv
}
