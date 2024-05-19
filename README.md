# Mit6.5840Lab(2024)

### Lab 1: MapReduce

##### 要求

你的任务是实现一个分布式 MapReduce，包括两个程序，协调器和工作者。将只有一个协调器进程，以及一个或多个并行执行的工作者进程。在真实系统中，工作者会运行在许多不同的机器上，但在这个实验中，你将把它们都运行在单台机器上。工作者将通过 RPC 与协调器通信。每个工作者进程都会循环地向协调器请求任务，从一个或多个文件中读取任务的输入，执行任务，将任务的输出写入一个或多个文件，并再次向协调器请求新任务。协调器应该注意到如果一个工作者在合理的时间内（在这个实验中，使用十秒）没有完成其任务，则将相同的任务分配给另一个工作者。

我们为你提供了一些初始代码。协调器和工作者的 "main" 程序位于 `main/mrcoordinator.go` 和 `main/mrworker.go` 中；请勿更改这些文件。你应该将你的实现放在 `mr/coordinator.go`、`mr/worker.go` 和 `mr/rpc.go` 中。

##### MapReduce流程总览

![image-20240519153530266](photo\image-20240519153530266.png)

##### 数据结构设计

~~~go
type Task struct{
	Type 		int 		// 0-Map 1-Reduce 2-End
	Id 			int			// Id
	Done        bool 		// 0-Begin 1-Running 2-End
	FileName 	string 		// 文件名称
	ReduceNum   int 		// Reduce任务数量
	MapNum      int 		// Map任务数量
	startAt 	time.Time 	// 开始时间
}

type Coordinator struct {
	// Your definitions here.
	mutex        		sync.Mutex
	TaskReduceChan 		[]Task 	// Reduce任务
	TaskMapChan    		[]Task 	// Map任务
	HaveFinMap 			int	   	// 已经完成Map任务数量
	HaveFinReduce 		int		// 已经完成Reduce任务数量
	MapNum      		int 	// Map任务数量
	ReduceNum  			int 	// Reduce任务数量
}
~~~

##### 主要实现流程

- `Coordinator`
  - `GetTask`，先派送`Map`任务，如果检测到`Map`任务全部完成后开始派送`Reduce`任务
    - 派送任务时通过`Done`和`startAT`字段来判断任务是否可派，超时未完成的任务也可继续派送
  - `FinMapTask`和`FinReduceTask`方法，收到`Worker`完成任务后进行后续处理
- `Worker`
  - `CallTask RPC`方法，获取任务后根据type执行相应的`Map`或`Reduce`方法
  - `CallFinMap RPC`和`CallFinReduce RPC`方法，通知`Coordinator`当前`Task`完成
  - `Map`方法
    - 首先，打开文件 `Task.FileName` 
    - 然后，使用 `ioutil.ReadAll` 函数读取文件的全部内容到 `content` 变量中，关闭已打开的文件，释放资源。
    - 接下来，通过调用 `mapf` 函数对文件内容进行映射处理，生成键值对数组 `kva`
    - 接着，代码创建了一个长度为 `ReduceNum` 的切片 `HashedKV`，用于存储经过哈希映射后的键值对
    - 遍历 `kva` 中的键值对，计算每个键对应的哈希值，并通过取余运算将其分配到对应的 Reduce 任务中。分配的方式是将键值对添加到 `HashedKV` 切片中对应的索引位置。
    - 之后，再次遍历长度为 `ReduceNum` 的切片，对每个 Reduce 任务创建输出文件，并将对应的键值对编码为 JSON 格式写入文件。文件名以 `"mr-" + strconv.Itoa(Task.Id) + "-" + strconv.Itoa(i)` 的形式命名，其中 `Task.Id` 表示任务的唯一标识。
    - 最后`CallFinMap(Task.Id)`
  - `Coordinator`方法
    - 首先，定义了一个空的键值对切片 `intermediate`，用于存储所有 Map 任务产生的中间结果
    - 接着，通过循环遍历所有的 Map 任务产生的文件，文件名格式为 `"mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(Task.Id)`，其中 `MapNum` 为预定义的常量，表示 Map 的数量，`Task.Id` 是当前任务的唯一标识。对于每个文件，打开并解码其中的键值对，将其添加到 `intermediate` 中。
    - 读取所有文件中的键值对后，对 `intermediate` 中的键值对按照键进行排序，以便后续的 Reduce 操作
    - 然后，创建输出文件，文件名格式为 `"mr-out-" + strconv.Itoa(Task.Id)`，其中 `Task.Id` 是当前任务的唯一标识。该输出文件将保存 Reduce 操作的结果
    - 在排序后的 `intermediate` 中进行迭代，对每个不同的键执行 Reduce 函数 `reducef`，将其对应的值列表作为参数传递给 `reducef` 函数，并将其返回的结果写入输出文件中。在写入结果时，使用格式化字符串 `"%v %v\n"`，将键和对应的输出值写入文件
    - 最后`CallFinReduce(Task.Id)`

##### 测试结果

![image-20240519160610837](photo\image-20240519160610837.png)

### Lab 2: Key/Value Server

该实验比较容易，不过多介绍

##### 数据结构设计

```go
type Clerk struct {
	server *labrpc.ClientEnd
	id  int64        // client id
	seq int64 // sequence number, increase by 1 for each request
}

type KVServer struct {
	mu sync.Mutex
	mmap map[string]string
	lastClientOp map[int64]op // last operation result for each client
}
```

##### 主要实现流程

- `Client`
  - `Get`、`Put`、`Append`方法
    - 通过`Key`、`Value`、`ClientId`、`Seq`参数循环向Server发出RPC请求，收到回复后结束
- `Server`
  - `Get`
    - 尝试从mmap中获取key对应的value，获取成功则返回value
  - `Put`
    - 判断是否重复操作
    - 将key对应value置为参数的value
  - `Append`
    - 判断是否重复操作
    - 将key对应value追加参数的value

##### 测试结果

![image-20240519163118210](photo\image-20240519163118210.png)



### Lab 3: Raft

##### 要求

- 3A：实现Raft的领导者选举和心跳（`AppendEntries RPC`，不包含日志条目）。Part 3A的目标是选举出单个领导者，如果没有故障，领导者保持领导地位，如果旧领导者失败或者与旧领导者之间的数据包丢失，则新领导者接管。运行`go test -run 3A`来测试你的3A代码。
- 3B：实现领导者和跟随者代码以追加新的日志条目，使得`go test -run 3B`测试通过。
- 3C：在`raft.go`中完成`persist()`和`readPersist()`函数，通过添加代码保存和恢复持久化状态。你需要将状态编码（或"序列化"）为字节数组，以便将其传递给Persister。使用labgob编码器；参考`persist()`和`readPersist()`中的注释。labgob类似于Go的gob编码器，但如果尝试对具有小写字段名称的结构进行编码，则会打印错误消息。目前，将nil作为第二个参数传递给`persister.Save()`。在你的实现更改持久化状态的地方插入调用`persist()`。一旦完成了这一步，如果你的其余实现是正确的，你应该通过所有的3C测试。
- 3D：实现Snapshot()和InstallSnapshot RPC，以及Raft支持这些变化的更改（例如，操作被修剪的日志）。当你的解决方案通过了3D测试（以及之前的Lab 3测试）时，你的解决方案就是完整的。

##### Raft consensus algorithm

![image-20240519212224917](photo\image-20240519212224917.png)

##### 数据结构设计

~~~go
type LogEntry struct {
	Command interface{} 		  //日志记录的命令(用于应用服务的命令)
	Index   int         		  //该日志的索引
	Term    int         		  //该日志被接收的时候的Leader任期
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
    
	currentTerm 	int
	votedFor    	int
	role      		int
	electionTimer   time.Time
	havevoted		int

	log				[]LogEntry
	commitIndex 	int				// 当前log中的最高索引(从0开始,递增)
	lastApplied		int				// 当前被用到状态机中的日志最高索引(从0开始,递增)
	nextIndex		[]int  			// 发送给每台服务器的下一条日志目录索引(初始值为leader的commitIndex + 1)
	matchIndex		[]int			// 每台服务器已知的已被复制的最高日志条目索引

	applyCh			chan ApplyMsg	// 存储machine state

	lastIncludeIndex int 			// 快照中包含的最后日志条目的索引值
	lastIncludeTerm  int			// 快照中包含的最后日志条目的任期号
}

const (
	Follower          int = 0
	Candidate         int = 1
	Leader            int = 2
	APPLIEDSLEEP 	  int = 20
	// 随机生成投票过期时间范围: MoreVoteTime+MinVoteTime ~ MinVoteTime
	MoreVoteTime 	  int = 120
	MinVoteTime       int = 80
	HeartbeatSleep    int = 35
)
~~~

##### 主要实现流程

- **发起选举**

  - ```go
    type RequestVoteArgs struct {
    	Term 			int
    	LeaderId 		int
    	LastLogIndex  	int
    	LastLogTerm 	int     
    }
    
    type RequestVoteReply struct {
    	Term 			int
    	VoteGranted 	bool
    }
    ```

  - 起一个`goroutine`循环检测是否不为`Leder`且`electionTimer`已过期，进入`CandidateTick`

  - 将`currentTerm`加一重置状态开始进行选举，起多个`goroutine`向同伴发起`RequestVote`

  - 如果获得超半数投票，则当选为`Leader`，设置`nextIndex`、`matchIndex`等信息后，开始发起`LeaderTick`

    - 如果收到回复的`Term`大于当前`Term`，停止选举，重置为`Follower`
    - 有许多细节的重置信息需要反复`debug`来修正，尤其时`votedFor`置-1，选举成功不能置为-1，否则会出现脑裂现象
    - 同时注意判断后续选举者状态是否对应选举开始状态

- **发送心跳**

  - ```go
    type AppendEntriesArgs struct {
    	Term 			int
    	LeaderId 		int
    	PrevLogIndex  	int			//Leader以为的其上条log的index
        PrevLogTerm 	int  		//Leader以为的其上条log的term
    	Entries			[]LogEntry
    	LeaderCommit	int
    }
    
    type AppendEntriesReply struct {
    	Term 			int
    	Success 		bool
    	ConflictIndex	int
    	ConflictTerm	int
    }
    ```

  - 起一个`goroutine`循环检测是否为`Leder`，进入`LeaderTick`

  - 通过`nextIndex`来获取需要携带的`log`信息、`PrevLogIndex`、`PrevLogTerm`

  - 起多个`goroutine`向同伴发起`AppendEntries`

    - 如果收到回复的`Term`大于当前`Term`，停止发送心跳，重置为`Follower`
    - 返回的任期已经落后当前任期，直接`return`
    - `Success`时进行更新`nextIndex`、`matchIndex`，
    - 否则处理日志冲突
      - 如果`ConflictTerm`为0，直接设置对应`nextindex`为`ConflictIndex+1`
      - `ConflictTerm`不为0，从后向前搜索`logterm`等于`ConflictTerm`的最后一个`logindex`(冲突快速更新策略)
      - 设置对应`nextindex`为搜索出的`logindex＋1`
    - 最后通过`matchIndex`判断是否有`log`可以进行`commit`，如果有半数以上同伴同意，更新`commit`

- **回复选举**

  - 如果当前`Term`大于选举者的`Term`，直接`false`返回

  - 重置信息

  - 如果当前`lastIncludeTerm`大于选举者，或`lastIncludeTerm`相等但`lastIncludeIndex`大于选举者，直接`false`返回

    如果`voteFor`为其他人，直接`false`返回

  - 否则将票投给他，并且更新自身的`electionTimer`等信息

- **回复心跳**

  - 如果当前`Term`大于选举者的`Term`，直接`false`返回

  - 重置信息。

  - 日志冲突判断

    - 如果`PrevLogIndex`小于当前`commitindex`，返回冲突日志`index`为`commitindex`，更新leader的`nextindex`
    - 如果`PrevLogIndex`小于当前`lastIncludeIndex`，返回冲突日志`index`为`lastIncludeIndex`，更新leader的`nextindex`
    - 如果`PrevLogIndex`大于当前`lastlogindex`，说明日志缺少，返回冲突日志`index`为当前`lastlogindex`，更新leader的`nextindex`
    - 置`curterm`为当前log中`PrevLogIndex`对应的`Term`（冲突快速更新策略）
      - 如果`PrevLogTerm`不等于`curterm`，反向搜索log中`Term`不等于`curterm`的`index`
      - 返回冲突日志`index`为搜索出的`index`，更新leader的`nextindex`

    将log从`PrevLogIndex`截断，并`append`上传来的`log`

  - 更新当前的`commitindex`为`min(leader的commitindex，lastlogindex)`

- **日志提交**
  
  - 起一个`goroutine`睡眠循环检测是否`lastApplied` < `commitIndex`
  - 将未提交的`commitIndex - lastApplied`个`log`进行提交
  - 注意先将`log`深拷贝出来，然后释放锁
  - 再向`chan`里放，否则会出现阻塞问题
  
- **持久化**

  - 将`log、currentTerm、votedFor、lastIncludeIndex、lastIncludeTerm`字段内容进行持久化
  - 注意在每个设置到上述字段修改的地方，都要调用持久化

- **快照**

  - 将log截断存储到`snap`中，并更新`lastIncludeIndex`、`lastIncludeTerm`
  - 由于log中内容减少，`index`和`term`的获取要对应进行修正，这点设置许多细节问题，需要注意
  - 在发送心跳的时候如果`prelogindex`小于`lastIncludeIndex`，则将自身的`Snapshot`发送
  - 同时提供了`Snapshot`接口，通过传入`index`和`snapshot`让`leader`主动进行快照

##### 测试结果

![image-20240519194943785](photo\image-20240519194943785.png)

![image-20240519194958610](photo\image-20240519194958610.png)

### Lab 4: Fault-tolerant Key/Value Service

##### 要求

- 你的第一个任务是实现一个在没有丢失消息和服务器故障的情况下正常工作的解决方案
- 随意将你的客户端代码从 `Lab 2 (kvsrv/client.go)` 复制到 `kvraft/client.go`。你需要添加逻辑来决定将每个 RPC 发送到哪个 `kvserver`。请注意，Recall that `Append()` no longer returns a value to the `Clerk`
- 你还需要在 `server.go` 中实现 `Put()`、`Append()` 和 `Get()` RPC 处理程序。这些处理程序应该使用 `Start()` 在 Raft 日志中输入一个 `Op`；你应该在 `server.go` 中填写 `Op` 结构的定义，使其描述一个 `Put/Append/Get` 操作。每个服务器应该在 Raft 提交它们时执行 `Op` 命令，即当它们出现在 `applyCh` 上时。RPC 处理程序应该注意到 Raft 提交其 `Op`，并回复 RPC
- 当你能可靠地通过测试套件中的第一个测试：“One client”时，你已经完成了这个任务。 添加代码来处理故障，并处理重复的 `Clerk` 请求，包括 `Clerk` 在一个任期中将请求发送到 `kvserver` leader，等待回复超时，然后将请求重新发送到另一个任期的新 leader 的情况。请求应该只执行一次。这些注释包括关于重复检测的指导。你的代码应该通过 `go test -run 4A tests`
- 修改你的 `kvserver`，使其能够检测到持久化的 Raft 状态增长过大，然后将快照传递给 Raft。当一个 `kvserver` 重新启动时，它应该从持久化器（`persister`）中读取快照，并从快照中恢复其状态

##### 数据结构设计

~~~go
type Clerk struct {
	servers 		[]*labrpc.ClientEnd
	clientid		int64
	seq				int64
	leaderserver	int64
}

type Op struct {
    // Raft start使用的op
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
	dead    	int32 	// set by Kill()

	maxraftstate int 	// snapshot if //log grows this big

	// Your definitions here.
	kvMap		map[string]string
	seqMap		map[int64]int64
	replyMap	map[IndexAndTerm]chan OpAppendReply
	lastApplied int
}

type IndexAndTerm struct {
    // 利用index和term来构造接收结果的chan，要及时删除
    Index int
    Term  int
}
~~~

##### 主要实现流程

**Client**

- ~~~go
  for {
      response := OpAppendReply{}
      ok := ck.servers[ck.leaderserver].Call("KVServer.Op", &args, &response)
      if ok{
          if response.Err == OK{
              return response.Value
          }else if response.Err == ErrNoKey{
              return ""
          }else if response.Err == ErrWrongLeader{
              ck.leaderserver = (ck.leaderserver + 1) % int64(len(ck.servers))
              continue
          }
      }
      ck.leaderserver = (ck.leaderserver + 1) % int64(len(ck.servers))
  }
  ~~~

- 如上将put append get方法统一到了一起，循环发送RPC请求给server，得到回复后结束

**Server**

- `Op`方法接收处理RPC请求

  - 如果不是Leader，直接返回`ErrWrongLeader`
  - 构造`op`结构体，发送`raft.Start(op)`，之后定时等待`index`和`term`构造的`chan`返回结构
  - 等到了返回结果，没等到超时就返回错误

- 起一个`applyOp()` `goroutine`，监测`raft`的`applyCh`时候有命令

  - 根据命令的`Do`参数去进行不同处理

  - 如果`RaftStateSize`大于`maxraftstate`阈值，保存当前`snapshot`，并通过`raft`的`Snapshot`接口发送

  - 根据`index`和`term`向`chan`写入结果，对应上面

  - 如果是`snapshot`的话，进行`DecodeSnapShot`并更新`lastApplied`

- `PersistSnapShot()`
  - 将`kvMap`和`seqMap`进行持久化，对对应修改的地方都要进行存储
  - `server`初始化时如果是重连，要进行`DecodeSnapShot`

##### 测试结果

![image-20240519213734249](photo\image-20240519213734249.png)

![image-20240519213747165](photo\image-20240519213747165.png)

- 前面一直出现如上错误too slowly

- 解决方法：在raft中start时，最后直接调用leadertick进行同步

![image-20240519214003114](photo\image-20240519214003114.png)

### Lab 5: Sharded Key/Value Service

##### 5A要求

- 你必须在 shardctrler/ 目录下的` client.go` 和 `server.go` 中实现上述指定的接口。你的 `shardctrler` 必须是容错的，使用你在实验 3/4 中的 Raft 库。当你通过 shardctrler/ 中的所有测试时，你已经完成了这个任务。

##### 5A数据结构设计

~~~go
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	clientid		int64
	seq				int64
	leaderserver	int64
}

type ShardCtrler struct {
	mu      	sync.Mutex
	me     		int
	rf      	*raft.Raft
	applyCh 	chan raft.ApplyMsg
	configs 	[]Config // indexed by config num
	seqMap		map[int64]int64
	replyMap	map[int]chan Op
}

type Op struct {
	ClientId 	int64				// 用户ID
    Seq      	int64				// cmd的ID
    Do			string				// Query Join Leave Move
	QueryNum	int					// QueryArgs Num
    JoinServers map[int][]string	// JoinArgs Servers
	LeaveGIDs 	[]int				// LeaveArgs GIDs
	MoveShard 	int					// MoveArgs Shard
	MoveGID   	int					// MoveArgs GID
}
~~~

##### 5A主要实现流程

本实验实在lab2基础上，对于不同的key进行分片，不同分片交由不同的GID来管理，每个GID又包含多个server，本实验主要实现对分片的管理以及负载均衡

**Client**

- `Query`、`Join`、`Leave`、`Move`方法

- ~~~go
  // Query请求的部分代码
  for {
      response := QueryReply{}
      ok := ck.servers[ck.leaderserver].Call("ShardCtrler.Query", args, &response)
      if ok{
          if response.Err == OK{
              return response.Config
          }else if response.Err == ErrWrongLeader{
              ck.leaderserver = (ck.leaderserver + 1) % int64(len(ck.servers))
              continue
          }
      }
      ck.leaderserver = (ck.leaderserver + 1) % int64(len(ck.servers))
      time.Sleep(100 * time.Millisecond)
  }
  ~~~

- 如上代码循环进行RPC请求，获得回复后退出

  - Query：通过`Key`、`Seq`、`Num`参数查询Config，成功后返回Config
  - Join：通过`Key`、`Seq`、`servers`参数，请求服务器将servers加入
  - Leave：通过`Key`、`Seq`、`GIDs`参数，请求服务器将GIDs中gid删除
  - Move：通过`Key`、`Seq`、`Shard`、`GID`参数，请求服务器将Shard分片分配给GID

**Server**

- 对应Client的RPC请求，四个类似的方法来处理
  - 根据收到的不同请求，构造统一个`op`结构体，然后调用`raft.Start(op)`，等待`replyChan`返回结果（超时不候）
  - `raft`同步后传入`applyCh`
  - `applyOp()`方法监听`applyCh`，根据不同的`Do`参数去分别进行`Query`、`Join`、`Leave`、`Move`处理
  - 相应方法处理后，将处理结果传入`replyChan`，上面RPC方法收到结果，然会给Client
- `JoinHandler`
  - 将入参中的`servers`不在`config.Group`中的加入进去
- `LeaveHandler`
  - 将真实存在的入参中的`gids`删除掉
- `MoveHandler`
  - `config.Shards[shard] = gid`
- `CommonRebalance`
  - 上述四个方法执行完成后，都要调用这个方法进行负载均衡，要求不同组负责的分片数量不能相差大于1
  - 首先实现一个找出拥有最多和最少分片的组的方法
  - 找出空闲未分配的分片，每次都分配给当前最少分片的组
  - 循环将拥有最多分片的组分一个给最少的组，直到**不满足**`max_gid_num > (min_gid_num + 1)`，结束
- 注意在进行map复制的时候，要再取k、v进行复制，不然直接x=y只是地址一样，会出现错误

##### 5B要求

- 在配置更改期间实现分片迁移。确保副本组中的所有服务器在它们执行的操作序列中的同一点进行迁移，以便它们都接受或拒绝并发客户端请求。
- 在处理后续测试之前，你应该专注于通过第二个测试（"加入然后离开"）。 
- 在通过测试中除了 `TestDelete `之外的所有测试后，你就完成了这个任务。

##### 5B数据结构设计

~~~GO
type Clerk struct {
	sm       		*shardctrler.Clerk
	config   		shardctrler.Config
	make_end 		func(string) *labrpc.ClientEnd
	seq				int64
	leaderserver	int64
	clientid		int64
}

type Op struct {
	// Raft start使用的op
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
	dead    	 int32 // set by Kill()
    mck          *shardctrler.Clerk // sck is a client used to contact shard master

	// Your definitions here.
	seqMap		map[int64]int64
	replyMap	map[int]chan OpAppendReply
	config		shardctrler.Config 	// 当前的config
	lastconfig	shardctrler.Config  // 上一份config
	shards 		[]Shard				
}

type Shard struct{
	StateMachine map[string]string  // 每个分片的kvMap
	ConfigNum    int 				// version
}
~~~

##### 5B主要实现流程

**Client**

- 类似`Lab4`中的`get`、`put`、`append`三个操作
- 开始利用`shard := key2shard(key)`，将该RPC传给对应的`Group`
- 每次循环后使用`ck.config = ck.sm.Query(-1)`，更新`config`

**Server**

- 类似`Lab4`中的对`get`、`put`、`append`三个操作RPC回应处理
- 增加`isMatchShard`判断该`server`是否拥有该分片，且`kv.shards[id].StateMachine`是否不为`nil`，该操作在`start`前后都要判断，不通过直接返回`ErrWrongGroup`，否则在网络错乱情况下会出错
- 跟`Lab4`最主要的区别还有要对分片进行管理，起一个`DetectConfig()` `goroutine`来对分片进行管理
  - 开始先判断是否有不属于当前的分片还持有
    - 通过新的`config`，将分片的`shard`信息和`seqMap`信息通过`AskShard` RPC请求发给对应的`server`
    - 对方`Leader`收到后，跟`get`方法类似，起一个`raft.start(op)`，在`applyCh`接收到命令后真正执行
    - 完成后将自身的`shard`删除，也是通过起一个`raft.start(op)`
    - `sleep`加`continue`
  - 再判断是否当前该获取的`shard`已经获取，否则`sleep`加`continue`
  - 到这里当前`shard`已经是当前`config`准确的状态
  - 将`config Num`加一，进行`Query`，如果真的获取到了新的`config`，进行更新当前`config`
- `UpdateConfig`
  - 如果`config num`小于等于当前，返回错误
  - `lastconfig`更新为`config`，`config`更新为`new config`
  - 将属于自己的分片且之前没被拥有过的，进行初始化
- `AddShard`
  - 如果`cmd.Seq < int64(kv.config.Num) || kv.shards[cmd.ShardId].StateMachine != nil`，直接`break`
  - 将分片的`StateMachine`更新，且将`SeqMap`中大于当前或者不存在更新
- `RemoveShard`
  - 如果`cmd.Seq < int64(kv.config.Num) || kv.shards[cmd.ShardId].StateMachine == nil`，直接`break`
  - 将`shard`的`StateMachine`置为`nil`
  - 将`shard`的`ConfigNum`置为`int(cmd.Seq)`

##### 测试结果

![image-20240519213645569](photo\image-20240519213645569.png)







