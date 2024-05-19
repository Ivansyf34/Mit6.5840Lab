package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ShardNotReady  = "ShardNotReady"
	ErrOverTime	   = "ErrOverTime"
	ConfigOut	   = "ErrOverTime"
)

const (
	DetectConfigTimeout  = 100 
	WaitStart = 500
)

type Err string

type OpAppendArgs struct {
	Key   		string
	Value 		string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId 	int64
	Seq      	int64
	Op			string
}

type OpAppendReply struct {
	Err Err
	Value string
}

type AskShardArgs struct {
	ShardId 	 int
	ClientId     int64
	RequestId    int64
	SeqMap		 map[int64]int64
	Shard		 Shard
	ConfigNum 	 int
}

type AskShardReply struct {
	Err			 Err
}

