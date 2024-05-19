package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "strconv"
import "encoding/json"
import "io/ioutil"
import "time"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for{
		Task := CallTask()
		//MapTask
		ReduceNum := Task.ReduceNum
		MapNum := Task.MapNum
		if(Task.Type == 0 && Task.FileName != ""){
			file, err := os.Open(Task.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", Task.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", Task.FileName)
			}
			file.Close()
			kva := mapf(Task.FileName, string(content))
			
			HashedKV := make([][]KeyValue, ReduceNum)

			for _,kv := range kva{
				hash_temp := ihash(kv.Key)%ReduceNum
				HashedKV[hash_temp] = append(HashedKV[hash_temp], kv)
			}

			for i := 0; i < ReduceNum; i++{
				oname := "mr-" + strconv.Itoa(Task.Id) + "-" + strconv.Itoa(i)
				ofile, _ := os.Create(oname)
				enc := json.NewEncoder(ofile)
				for _, kv := range HashedKV[i] {
					enc.Encode(kv)
				}
				// enc.Encode(HashedKV[i])
				ofile.Close()
			}
			CallFinMap(Task.Id)
		}else if(Task.Type == 1){
			// Reduce Task
			intermediate:= []KeyValue{}
			for i:=0; i<MapNum; i++{
				filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(Task.Id)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				defer file.Close()

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
					break
					}
					intermediate = append(intermediate, kv)
				}
			}

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", Task.Id)
			ofile, _ := os.Create(oname)
			defer ofile.Close()

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			CallFinReduce(Task.Id)
		}else if(Task.Type == 2){
			// Wait
			time.Sleep(500 * time.Millisecond)
		}else{
			return
		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallTask() Task{
	// declare an argument structure.
	args := TaskRequest{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.GetTask", &args, &reply)

	// reply.Y should be 100.
	if ok {
		// fmt.Printf("reply.Task.FileName %s\n", reply.Task.FileName
	} else{
		fmt.Printf("Call Failed!\n")
	}
	return reply.Task
}

func CallFinMap(Id int){
	args := MapFinRequest{}
	// declare a reply structure.
	args.Id = Id
	reply := MapFinReply{}
	// send the RPC request, wait for the reply.
	call("Coordinator.FinMapTask", &args, &reply)
}

func CallFinReduce(Id int){
	args := MapFinRequest{}
	args.Id = Id
	// declare a reply structure.
	reply := MapFinReply{}
	// send the RPC request, wait for the reply.
	call("Coordinator.FinReduceTask", &args, &reply)
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
