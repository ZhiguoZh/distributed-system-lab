package mr

import "encoding/json"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "sort"
import "time"


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
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

	for {
		AskForTask(worker_id, &task, &nReduce)
		// Simulating process
		//time.Sleep(1 * time.Second)

		// Real execution
		if task.Type == "Map" && len(task.Files) == 1 {
			file, err := os.Open(task.Files[0])
			if err != nil {
				log.Fatalf("cannot open %v", task.Files[0])
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.Files[0])
			}
			file.Close()
			kva := mapf(task.Files[0], string(content))

			var out_files []*os.File
			var encoders []*json.Encoder
			for i := 0; i < nReduce; i += 1 {
				// Leave file name empty here, then rename the whole name afterwards.
				tmp_file, err := ioutil.TempFile("", "")
				if err != nil {
					log.Fatalf("cannot create temp file")
				}
				out_files = append(out_files, tmp_file)
				//out_files[i] = ioutil.TempFile("", filename)
				encoders = append(encoders, json.NewEncoder(tmp_file))
				//encoders[i] = json.NewEncoder(out_files[i])
			}

			for _, kv := range(kva) {
				idx := ihash(kv.Key) % nReduce
				if err := encoders[idx].Encode(&kv); err != nil {
					break
				}
			}
			// Leave temp file renaming for future use.
			// os.File.Name() contains the whole path of the file. 

			var results []string
			for i := 0; i < nReduce; i += 1 {
				filename := fmt.Sprintf("%s-%d-%d", "mr", task.Tid, i)
				if err := os.Rename(out_files[i].Name(), filename); err != nil {
					log.Fatalf("cannot rename file %v", filename)
				}
				results = append(results, filename)
			}
			NotifyComplete(worker_id, task.Tid, task.Type, results)

		} else if task.Type == "Reduce" {
			intermediate := []KeyValue{}
			for _,f := range(task.Files) {
				file, err := os.Open(f)
				if err != nil {
					log.Fatalf("cannot open %v", task.Files[0])
				}
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

			ofile, err := ioutil.TempFile("", "")
			if err != nil {
				log.Fatalf("cannot create temp file")
			}

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

			filename := fmt.Sprintf("mr-out-%d", task.Tid)
			if err := os.Rename(ofile.Name(), filename); err != nil {
				log.Fatalf("cannot rename file %v", filename)
			}
			ofile.Close()

			NotifyComplete(worker_id, task.Tid, task.Type, nil)
		}
		//NotifyComplete(worker_id, task.Tid, task.Type, []string{"mr-2-4", "mr-3-5", "mr-10-5"})
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
