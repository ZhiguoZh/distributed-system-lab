package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	worker_id := os.Getpid()

	task := Task{}
	nReduce := 0

	for i := 0; i < 20; i += 1{
		AskForTask(worker_id, &task, &nReduce)
		//TODO: AskForTask should return a task for later process
		//Process
		//Notify complete
		// Test: Complete 12s later
		//time.Sleep(12 * time.Second)
		NotifyComplete(worker_id, task.Tid, task.Type, []string{"mr-2-4", "mr-3-5", "mr-10-5"})
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()
	// Test: Complete immediately
	//NotifyComplete(worker_id, []string{"mr-2-4", "mr-3-5", "mr-10-5"})

	// Test: Complete 12s later
	//time.Sleep(12 * time.Second)
	//NotifyComplete(worker_id, []string{"mr-2-4", "mr-3-5", "mr-10-5"})
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func AskForTask(worker_id int, task *Task, nReduce *int) {
	args := GetTaskArgs{worker_id}
	reply := GetTaskReply{}

	for {
		if call("Master.AssignTask", &args, &reply) == false {
			os.Exit(1)
		}
		if (reply.Msg == "Task") {
			break
		}
		time.Sleep(2 * time.Second)
	}

	task.Tid = reply.TaskId
	task.Type = reply.Type
	task.Files = reply.Filelist

	*nReduce = reply.NReduce
}

func NotifyComplete(worker_id int, task_id int, task_type string, results []string) {
	args := CompleteArgs{worker_id, task_id, task_type, results}
	if call("Master.WorkerComplete", &args, nil) == false {
		os.Exit(1)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
