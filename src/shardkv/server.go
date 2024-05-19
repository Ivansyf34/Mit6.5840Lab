package shardkv


import "6.5840/labrpc"
import "6.5840/raft"
import "sync"
import "6.5840/labgob"
import "6.5840/shardctrler"
import "time"
import "sync/atomic"
import "bytes"
//import "log"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId 	int64
	Seq      	int64
	Do			string
	Key			string
	Value		string
	Config		shardctrler.Config
	Shard		Shard
	ShardId		int
	SeqMap		map[int64]int64
}

type ShardKV struct {
	mu           sync.Mutex
	migratemu    sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead    	int32 // set by Kill()

	// Your definitions here.
	seqMap		map[int64]int64
	replyMap	map[int]chan OpAppendReply

	config		shardctrler.Config
	lastconfig	shardctrler.Config
	shards 		[]Shard
	mck           *shardctrler.Clerk // sck is a client used to contact shard master
}

type Shard struct{
	StateMachine map[string]string
	ConfigNum    int // what version this Shard is in
}

func (kv *ShardKV) getChan(index int) chan OpAppendReply{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exist := kv.replyMap[index]
	if !exist{
		kv.replyMap[index] = make(chan OpAppendReply, 1)
	}
	return kv.replyMap[index]
}

func (kv *ShardKV) isDuplicateOp(op *Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastSeq, ok := kv.seqMap[op.ClientId]
	if !ok{
		return false
	}
	return lastSeq >= op.Seq
}

func (kv *ShardKV) isMatchShard(key string) bool {
	id := key2shard(key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Shards[id] == kv.gid && kv.shards[id].StateMachine != nil
}

func (kv *ShardKV) Op(args *OpAppendArgs, reply *OpAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if !kv.isMatchShard(args.Key){
		reply.Err = ErrWrongGroup
		//log.Printf("[Op] %d, reply.Err = ErrWrongGroup", kv.me)
		return
	}

	//log.Printf("[Op] %d, op = %s, key =%s, value = %s", kv.me, args.Op, args.Key, args.Value)
	cmd := Op{ClientId: args.ClientId, Seq: args.Seq, Do: args.Op, Key: args.Key, Value: args.Value}
	err := kv.Start(&cmd, reply)
	if err != OK {
		reply.Err = err
		return
	}

	if !kv.isMatchShard(args.Key){
		reply.Err = ErrWrongGroup
		//log.Printf("[Op] %d, reply.Err = ErrWrongGroup", kv.me)
		return
	}
    reply.Err = OK
	return
}

func (kv *ShardKV) AskShard(args *AskShardArgs, reply *AskShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//log.Printf("%d回应ask迁移的shard:%d, confignum:%d", kv.gid, args.ShardId, kv.config.Num)
	cmd := Op{ClientId: args.ClientId, Seq: args.RequestId, Do: "AddShard",ShardId: args.ShardId, Shard:args.Shard, SeqMap: args.SeqMap }
	reply.Err = kv.Start(&cmd, &OpAppendReply{})
	return
}

func (kv *ShardKV) DetectConfig(){
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			lastConfig := kv.lastconfig
			kv.mu.Unlock()
			if !kv.allSent(){
				for shard, gid := range lastConfig.Shards{
					//log.Printf("%d判断迁移: gid:%d kv.gid:%d curConfig.Shards[shard]:%d, %d < %d", kv.me, gid, kv.gid, kv.config.Shards[shard],kv.shards[shard].ConfigNum, kv.config.Num)
					kv.mu.Lock()
					if gid == kv.gid && kv.config.Shards[shard] != kv.gid && kv.shards[shard].ConfigNum < kv.config.Num{
						//log.Printf("%d发现需要ask迁移的shard:%d confignum:%d", kv.gid, shard, kv.config.Num)
						serversList := kv.config.Groups[kv.config.Shards[shard]]
						kv.mu.Unlock()
						servers := make([]*labrpc.ClientEnd, len(serversList))
						for i, name := range serversList {
							servers[i] = kv.make_end(name)
						}
						go func(servers []*labrpc.ClientEnd, shardId int) {
							index := 0
							start := time.Now()
							args := AskShardArgs{}
							kv.mu.Lock()
							args.ShardId =  shardId
							args.ClientId = int64(kv.gid)
							args.RequestId = int64(kv.config.Num)
							args.ConfigNum = kv.config.Num
							args.Shard = Shard{
								StateMachine: make(map[string]string),
								ConfigNum:    kv.config.Num,
							}
							for k, v := range kv.shards[shardId].StateMachine{
								//log.Printf("StateMachine-- key:%s value:%s",k,v)
								args.Shard.StateMachine[k] = v
							}
							args.SeqMap = make(map[int64]int64)
							for k, v := range kv.seqMap {
								args.SeqMap[k] = v
							}
							kv.mu.Unlock()
							for {		
								var reply AskShardReply
								ok := servers[index].Call("ShardKV.AskShard", &args, &reply)
								if ok && (reply.Err == OK || time.Since(start) >= 2*time.Second) {
									// 如果成功将切片发送给对应的服务器，GC 掉不属于自己的切片
									kv.mu.Lock()
									cmd := Op{ClientId: int64(kv.gid), Seq: int64(kv.config.Num), Do: "RemoveShard", ShardId: shardId}
									kv.mu.Unlock()
									kv.Start(&cmd, &OpAppendReply{})
									break
								}
								index = (index + 1) % len(servers)
								// 如果已经发送了一轮
								if index == 0 {
									time.Sleep(DetectConfigTimeout * time.Millisecond)
								}
							}
						}(servers, shard)
					}else{
						kv.mu.Unlock()
					}
				}
				time.Sleep(DetectConfigTimeout * time.Millisecond)
				continue
			}

			if !kv.allReceived() {
				time.Sleep(DetectConfigTimeout * time.Millisecond)
				continue
			}

			kv.mu.Lock()
			next := kv.config.Num + 1
			cfg := kv.mck.Query(next)
			if cfg.Num < next {
				kv.mu.Unlock()
				time.Sleep(DetectConfigTimeout * time.Millisecond)
				continue
			}	
			command := Op{
				Do:   "UpdateConfig",
				ClientId: int64(kv.gid),
				Seq:    int64(cfg.Num),
				Config: cfg,
			}
			kv.mu.Unlock()
			kv.Start(&command, &OpAppendReply{})
		}else{
			time.Sleep(DetectConfigTimeout  * time.Millisecond)
		}
	}
}

func (kv *ShardKV) Start(cmd *Op, reply *OpAppendReply) Err{
	index, _, isLeader := kv.rf.Start(*cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return ErrWrongLeader
	}
	//log.Printf("[Start] op:%s gid:%d",cmd.Do, kv.gid)
	ch := kv.getChan(index)
	// 设置超时ticker
	timer := time.NewTicker(WaitStart * time.Millisecond)
	defer func(){
		timer.Stop()
		kv.mu.Lock()
		delete(kv.replyMap, index)
		kv.mu.Unlock()
	}()

	select {
	case response := <-ch:
		//log.Printf("[Op] %d %s key = %s success, index:%d",kv.me, cmd.Do, cmd.Key, index)
		reply.Err = OK
		reply.Value = response.Value
		return OK
	case <-timer.C:
		//log.Printf("[Op] %d %s key = %s timeout, index:%d",kv.me, cmd.Do, cmd.Key, index)
		reply.Err = ErrOverTime
		return ErrOverTime
	}
}

func (kv *ShardKV) allReceived() bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for shard, gid := range kv.lastconfig.Shards{
		if gid != kv.gid && kv.config.Shards[shard] == kv.gid && kv.shards[shard].ConfigNum < kv.config.Num{
			return false
		}
	}
	return true
}

func (kv *ShardKV) allSent() bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for shard, gid := range kv.lastconfig.Shards{
		if gid == kv.gid && kv.config.Shards[shard] != kv.gid && kv.shards[shard].ConfigNum < kv.config.Num{
			return false
		}
	}
	return true
}

func (kv *ShardKV) ApplyOp() {
	for !kv.killed() {
		select{
		case msg := <-kv.applyCh:
			if msg.CommandValid{
				var cmd Op = msg.Command.(Op)
				op := cmd.Do
				//kv.lastApplied = msg.CommandIndex
				var response OpAppendReply
				if op == "Get" || op == "Put" || op == "Append"{
					//log.Printf("[ApplyOp] op = %s, gid:%d,commandindex = %d", op, kv.gid, msg.CommandIndex)
					shardId := key2shard(cmd.Key)
					if !kv.isMatchShard(cmd.Key){
						response = OpAppendReply{ErrWrongGroup, ""}
					}else{
						if kv.isDuplicateOp(&Op{ClientId: cmd.ClientId, Seq: cmd.Seq}) {
							response = OpAppendReply{OK, kv.shards[shardId].StateMachine[cmd.Key]}
						} else {
							kv.mu.Lock()
							kv.seqMap[cmd.ClientId] = cmd.Seq
							switch op {
							case "Put":
								kv.shards[shardId].StateMachine[cmd.Key] = cmd.Value
								response = OpAppendReply{OK, ""}
							case "Append":
								kv.shards[shardId].StateMachine[cmd.Key] += cmd.Value
								response = OpAppendReply{OK, ""}
							case "Get":
								if value, ok := kv.shards[shardId].StateMachine[cmd.Key]; ok {
									response = OpAppendReply{OK, value}
								} else {
									response = OpAppendReply{ErrNoKey, ""}
								}
							}
							kv.mu.Unlock()
						}
					}
				}else{
					kv.mu.Lock()
					switch op {
					case "UpdateConfig":
						curConfig := kv.config
						upConfig := cmd.Config
						if curConfig.Num >= upConfig.Num {
							response = OpAppendReply{ErrWrongGroup, ""}
						}else{
							kv.lastconfig = curConfig
							kv.config = upConfig
							for shard, gid := range upConfig.Shards {
								if gid == kv.gid && curConfig.Shards[shard] <= 0{
									kv.shards[shard].StateMachine = make(map[string]string)
									kv.shards[shard].ConfigNum = upConfig.Num
								}
							}
							response = OpAppendReply{OK, ""}
						}
						
					case "AddShard":
						if cmd.Seq < int64(kv.config.Num) || kv.shards[cmd.ShardId].StateMachine != nil{
							break
						}	
						kv.shards[cmd.ShardId].StateMachine = make(map[string]string)
						for k, v := range cmd.Shard.StateMachine {
							kv.shards[cmd.ShardId].StateMachine[k] = v
						}
						kv.shards[cmd.ShardId].ConfigNum = cmd.Shard.ConfigNum
						for k, v := range cmd.SeqMap {
							if r, ok := kv.seqMap[k]; !ok || r < v{
								kv.seqMap[k] = v
							}
						}
						//log.Printf("[AddShard] gid:%d, shard:%d confignum:%d",kv.gid, cmd.ShardId, kv.config.Num)
					case "RemoveShard":
						if cmd.Seq < int64(kv.config.Num) || kv.shards[cmd.ShardId].StateMachine == nil{
							break
						}
						kv.shards[cmd.ShardId].StateMachine = nil
						kv.shards[cmd.ShardId].ConfigNum = int(cmd.Seq)
						//log.Printf("[RemoveShard] gid:%d, shard:%d confignum:%d",kv.gid, cmd.ShardId, kv.config.Num)
					}
					kv.mu.Unlock()
				}

				// 超过阈值, 让rf进行snapshot
				if kv.maxraftstate != -1 && kv.maxraftstate < kv.rf.GetRaftStateSize(){
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}

				if _, isLeader := kv.rf.GetState(); isLeader{
					ch := kv.getChan(msg.CommandIndex)
					ch <- response
				}
			}else if msg.SnapshotValid{
				kv.mu.Lock()
				kv.DecodeSnapShot(msg.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shards []Shard
	var seqMap map[int64]int64
	var maxraftstate int
	var lastconfig shardctrler.Config
	var config shardctrler.Config
	//var lastApplied int

	if d.Decode(&shards) == nil && 
		d.Decode(&seqMap) == nil && 
		d.Decode(&maxraftstate) == nil && 
		d.Decode(&config) == nil && 
		d.Decode(&lastconfig) == nil {
		kv.shards = shards
		kv.seqMap = seqMap
		kv.maxraftstate = maxraftstate
		kv.config = config
		kv.lastconfig = lastconfig
	} else {
		//log.Printf("S%d Failed to decode snapshot", kv.me)
	}
}

func (kv *ShardKV) PersistSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	e.Encode(kv.seqMap)
	e.Encode(kv.maxraftstate)
	e.Encode(kv.config)
	e.Encode(kv.lastconfig)
	//e.Encode(kv.lastApplied)
	data := w.Bytes()
	return data
}


// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.shards = make([]Shard, shardctrler.NShards)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.seqMap = make(map[int64]int64)
	kv.replyMap = make(map[int]chan OpAppendReply)

	// 因为可能会crash重连
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	go kv.ApplyOp()
	go kv.DetectConfig()

	return kv
}
