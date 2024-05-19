package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "time"
import "sort"
//import "log"


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	seqMap		map[int64]int64
	replyMap	map[int]chan Op
}


type Op struct {
	// Your data here.
	ClientId 	int64
	Seq      	int64
	Do			string
	QueryNum	int
	JoinServers map[int][]string
	LeaveGIDs 	[]int
	MoveShard 	int
	MoveGID   	int
}

func (sc *ShardCtrler) getChan(index int) chan Op{
	//log.Printf("[getChan] begin!")
	sc.mu.Lock()
	defer sc.mu.Unlock()
	_, exist := sc.replyMap[index]
	if !exist{
		sc.replyMap[index] = make(chan Op, 1)
	}
	//log.Printf("[getChan] end!")
    
	return sc.replyMap[index]
}

func (sc *ShardCtrler) isDuplicateOp(op *Op) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastSeq, ok := sc.seqMap[op.ClientId]
	if !ok{
		return false
	}
	return lastSeq >= op.Seq
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{ClientId: args.ClientId, Seq: args.SeqId, Do: "Join", JoinServers: args.Servers}
	index, _, _ := sc.rf.Start(op)
	//log.Printf("[Op] %d, index = %d, term = %d, op = %s, key =%s, value = %s", sc.me, index, term,args.Op, args.Key, args.Value)

    ch := sc.getChan(index)
    defer func() {
        sc.mu.Lock()
        delete(sc.replyMap, index)
        sc.mu.Unlock()
    }()

	// 设置超时ticker
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()

    select {
    case response := <-ch:
		//log.Printf("[Op] %d success, term:%d, commandindex:%d", sc.me, it.Term, it.Index)
		if op.ClientId != response.ClientId || op.Seq != response.Seq {
			reply.Err = ErrWrongLeader
			return
		} else {
			reply.Err = OK
			return
		}
    case <-timer.C:
		//log.Printf("[Op] %d timeout, term:%d, commandindex:%d", sc.me, it.Term, it.Index)
		reply.Err = ErrWrongLeader
        return
    }
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{ClientId: args.ClientId, Seq: args.SeqId, Do: "Leave", LeaveGIDs: args.GIDs}
	index, _, _ := sc.rf.Start(op)
	//log.Printf("[Op] %d, index = %d, term = %d, op = %s, key =%s, value = %s", sc.me, index, term,args.Op, args.Key, args.Value)

    ch := sc.getChan(index)
    defer func() {
        sc.mu.Lock()
        delete(sc.replyMap, index)
        sc.mu.Unlock()
    }()

	// 设置超时ticker
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()

    select {
    case response := <-ch:
		//log.Printf("[Op] %d success, term:%d, commandindex:%d", sc.me, it.Term, it.Index)
		if op.ClientId != response.ClientId || op.Seq != response.Seq {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
    case <-timer.C:
		//log.Printf("[Op] %d timeout, term:%d, commandindex:%d", sc.me, it.Term, it.Index)
		reply.Err = ErrWrongLeader
        return
    }
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{ClientId: args.ClientId, Seq: args.SeqId, Do: "Move", MoveGID: args.GID, MoveShard: args.Shard}
	index, _, _ := sc.rf.Start(op)
	//log.Printf("[Op] %d, index = %d, term = %d, op = %s, key =%s, value = %s", sc.me, index, term,args.Op, args.Key, args.Value)

    ch := sc.getChan(index)
    defer func() {
        sc.mu.Lock()
        delete(sc.replyMap, index)
        sc.mu.Unlock()
    }()

	// 设置超时ticker
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()

    select {
    case response := <-ch:
		//log.Printf("[Op] %d success, term:%d, commandindex:%d", sc.me, it.Term, it.Index)
		if op.ClientId != response.ClientId || op.Seq != response.Seq {
			reply.Err = ErrWrongLeader
			return
		} else {
			reply.Err = OK
			return
		}
    case <-timer.C:
		//log.Printf("[Op] %d timeout, term:%d, commandindex:%d", sc.me, it.Term, it.Index)
		reply.Err = ErrWrongLeader
        return
    }
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	//log.Printf("[Query] %d", sc.me)
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//log.Printf("[Query] leader找到啦%d", sc.me)
	sc.mu.Lock()
	if(args.Num >= 0 && args.Num < len(sc.configs)){
		reply.Config =  sc.configs[args.Num]
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{ClientId: args.ClientId, Seq: args.SeqId, Do: "Query", QueryNum: args.Num}
	index, _, _ := sc.rf.Start(op)

	//log.Printf("[Op] %d, index = %d, op = query", sc.me, index)

    ch := sc.getChan(index)
    defer func() {
        sc.mu.Lock()
        delete(sc.replyMap, index)
        sc.mu.Unlock()
    }()

	// 设置超时ticker
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()

    select {
    case response := <-ch:
		//log.Printf("[Op] %d success, commandindex:%d", sc.me, index)
		if op.ClientId != response.ClientId || op.Seq != response.Seq {
			reply.Err = ErrWrongLeader
			return
		} else {
			sc.mu.Lock()
			reply.Err = OK
			sc.seqMap[op.ClientId] = op.Seq
			if op.QueryNum == -1 || op.QueryNum >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[op.QueryNum]
			}
			sc.mu.Unlock()
			return
		}
    case <-timer.C:
		//log.Printf("[Op] %d timeout, commandindex:%d", sc.me, index)
		reply.Err = ErrWrongLeader
        return
    }
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applyOp() {
	for{
		select{
		case msg := <-sc.applyCh:
			if msg.CommandValid{
				//log.Printf("[applyOp] %d,commandindex = %d", sc.me, msg.CommandIndex)
				var cmd Op = msg.Command.(Op)
				if cmd.Do!= "Query" && !sc.isDuplicateOp(&cmd){
					sc.mu.Lock()
					lastCfg := sc.configs[len(sc.configs)-1]
	
					// 复制上一个配置
					nextCfg := lastCfg
					nextCfg.Num += 1
					nextCfg.Groups = make(map[int][]string)
					for gid, servers := range lastCfg.Groups {
						nextCfg.Groups[gid] = append([]string{}, servers...)
					}
					switch cmd.Do{
					case "Join":
						sc.seqMap[cmd.ClientId] = cmd.Seq
						sc.JoinHandler(&nextCfg, cmd.JoinServers)
					case "Leave":
						sc.seqMap[cmd.ClientId] = cmd.Seq
						sc.LeaveHandler(&nextCfg, cmd.LeaveGIDs)
					case "Move":
						sc.seqMap[cmd.ClientId] = cmd.Seq
						sc.MoveHandler(&nextCfg, cmd.MoveGID, cmd.MoveShard)
					}
					sc.configs = append(sc.configs, nextCfg)
					sc.mu.Unlock()
				}
				sc.getChan(msg.CommandIndex) <- cmd
			}
		}
	}
}

func (sc *ShardCtrler) GetMaxMinGid(gid_shard map[int][]int) (int, int, int, int){
	temp_min := NShards + 1
	min_gid := -1
	temp_max := -1
	max_gid := -1
	keys := make([]int, 0)
    for k, _ := range gid_shard {
        keys = append(keys, k)
    }
	sort.Ints(keys)
	for _, gid := range keys{
		shards := gid_shard[gid]
		if len(shards) < temp_min{
			temp_min = len(shards)
			min_gid = gid
		}
		if len(shards) > temp_max{
			temp_max = len(shards)
			max_gid = gid
		}
	}
	return max_gid, min_gid, temp_max, temp_min
}

// 通过均衡算法
func (sc *ShardCtrler) CommonReblance(config *Config) {
	gid_shards := make(map[int][]int)
	free_shard := make([]int, NShards)
	for k, _ := range config.Groups{
		gid_shards[k] = make([]int, 0)
	}		

	for shard, gid := range config.Shards {
		_, ok := config.Groups[gid]
		if ok {
			gid_shards[gid] = append(gid_shards[gid], shard)
			free_shard[shard] = 1
		}
	}
	
	for i:=0 ; i < NShards; i++{
		if(free_shard[i] == 0){
			_, min_gid, _, _:= sc.GetMaxMinGid(gid_shards)
			config.Shards[i] = min_gid
			gid_shards[min_gid] = append(gid_shards[min_gid], i)
		}
	}

	max_gid, min_gid, max_gid_num, min_gid_num := sc.GetMaxMinGid(gid_shards)
	for max_gid_num > (min_gid_num + 1){
		len := len(gid_shards[max_gid])
		temp_shard := gid_shards[max_gid][len - 1]
		gid_shards[max_gid] = gid_shards[max_gid][:len - 1]
		gid_shards[min_gid] = append(gid_shards[min_gid], temp_shard)
		config.Shards[temp_shard] = min_gid

		max_gid, min_gid, max_gid_num, min_gid_num = sc.GetMaxMinGid(gid_shards)
	}
}

func (sc *ShardCtrler) JoinHandler(config *Config, joinservers map[int][]string){
	num := len(config.Groups)
	//log.Printf("[JoinHandler] begin: %d, join len:%d", num, len(joinservers))
	for gid, servers := range joinservers{
		_, exist := config.Groups[gid]
		if !exist{
			num++
		}
		newservers := make([]string, len(servers))
		copy(newservers, servers)
		config.Groups[gid] = newservers
	}
	if num == 0{
		return
	}
	sc.CommonReblance(config)
	//log.Printf("[JoinHandler] end: %d", len(config.Groups))
}

func (sc *ShardCtrler) LeaveHandler(config *Config, gids []int){
	num := len(config.Groups)
	//log.Printf("[LeaveHandler] begin: %d, leave len:%d", num, len(gids))
	for _, gid := range gids {
        if _, ok := config.Groups[gid]; ok {
            delete(config.Groups, gid)
            num--
        }
    }
    if num == 0 {
        config.Groups = map[int][]string{}
        return
    }
	sc.CommonReblance(config)
	//log.Printf("[LeaveHandler] end: %d", len(config.Groups))
}

func (sc *ShardCtrler) MoveHandler(config *Config, gid int, shard int){
	config.Shards[shard] = gid
	sc.CommonReblance(config)
}
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.seqMap = make(map[int64]int64)
	sc.replyMap = make(map[int]chan Op)
	go sc.applyOp()

	return sc
}
