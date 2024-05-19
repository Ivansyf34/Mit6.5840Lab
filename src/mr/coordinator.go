package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
//import "fmt"
import "sync"

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

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Coordinator) GetTask(args *TaskRequest, reply *TaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	now := time.Now()
	timeoutAgo := now.Add(-10 * time.Second)
	if(m.HaveFinMap < m.MapNum){
		for i:=0; i<m.MapNum; i++{
			t := m.TaskMapChan[i]
			if(!t.Done && t.startAt.Before(timeoutAgo)){
				reply.Task = t
				reply.Task.startAt = now
				m.TaskMapChan[i].startAt = now
				return nil
			}
		}
		reply.Task = Task{Type: 2}
	}else if(m.HaveFinReduce < m.ReduceNum){
		for i:=0; i<m.ReduceNum; i++{
			t := m.TaskReduceChan[i]
			if(!t.Done && t.startAt.Before(timeoutAgo)){
				reply.Task = t
				reply.Task.startAt = now
				m.TaskReduceChan[i].startAt = now
				return nil
			}
		}
		reply.Task = Task{Type: 2}
	}else{
		reply.Task = Task{Type: 3}
	}
	return nil
}

func (m *Coordinator) FinMapTask(args *MapFinRequest, reply *MapFinReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.TaskMapChan[args.Id].Done = true
	m.HaveFinMap++

	if(m.HaveFinMap == m.MapNum){
		for i:=0; i<m.ReduceNum; i++{
			m.TaskReduceChan[i] = Task{Type: 1, Id: i, Done: false, FileName: "", ReduceNum: m.ReduceNum, MapNum: m.MapNum}
		}
	}
	return nil
}

func (m *Coordinator) FinReduceTask(args *MapFinRequest, reply *MapFinReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.TaskReduceChan[args.Id].Done = true
	m.HaveFinReduce++
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Your code here.
	return c.HaveFinReduce == c.ReduceNum
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)
	c := Coordinator{ReduceNum: nReduce, 
				TaskReduceChan: make([]Task, nReduce),
				TaskMapChan: make([]Task, nMap),
				HaveFinMap: 0,
				HaveFinReduce: 0,
				MapNum: nMap}

	for i:=0; i<nMap; i++{ 
		c.TaskMapChan[i] = Task{Type: 0, Id: i, Done: false, FileName: files[i], ReduceNum: c.ReduceNum, MapNum: nMap}
	}

	// Your code here.


	c.server()
	return &c
}
