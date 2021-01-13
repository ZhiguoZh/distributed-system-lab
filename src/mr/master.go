package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "strconv"
import "strings"
import "time"


type Task struct {
	Tid int
	Type string
	Files []string
}

func (t *Task) Print() {
	fmt.Println("Tid:", t.Tid)
	fmt.Println("Type:", t.Type)
	fmt.Println("[")
	for _,f := range(t.Files) {
		fmt.Println(f)
	}
	fmt.Println("]")
}

type Master struct {
	// Your definitions here.
	mu sync.Mutex
	Phase string
	Unstarted_tasks []Task
	Wip_tasks map[int]Task
	Reduce_inputs [][]string
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) Print() {
	fmt.Println("Phase:", m.Phase)
	fmt.Println("# Unstarted Task:", len(m.Unstarted_tasks))
	for _,t := range(m.Unstarted_tasks) {
		t.Print()
	}
	fmt.Println("# Wip Tasks:", len(m.Wip_tasks))
	for worker_id,t := range(m.Wip_tasks) {
		fmt.Println("Worker ID:", worker_id)
		t.Print()
	}
	for reduce_id,inputs := range(m.Reduce_inputs) {
		fmt.Println("Reduce Id:", reduce_id)
		for _,f := range(inputs) {
			fmt.Println(f)
		}
	}
}

func (m *Master) AssignTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Println(args.WorkerId)

	if len(m.Unstarted_tasks) == 0 {
		reply.Msg = "No task"
		return nil
	}
	t := m.Unstarted_tasks[len(m.Unstarted_tasks)-1]
	m.Unstarted_tasks = m.Unstarted_tasks[:len(m.Unstarted_tasks)-1]
	reply.Msg = "Task"
	reply.TaskId = t.Tid
	reply.Type = t.Type
	reply.Filelist = t.Files
	reply.NReduce = len(m.Reduce_inputs)

	m.Wip_tasks[args.WorkerId] = t

	go func(m *Master, worker_id int) {
		time.Sleep(10 * time.Second)
		m.mu.Lock()
		//Test.
		//m.Print()
		if v,ok := m.Wip_tasks[worker_id]; ok == true {
			delete(m.Wip_tasks, worker_id)
			m.Unstarted_tasks = append(m.Unstarted_tasks, v)
		}
		//Test.
		//m.Print()
		m.mu.Unlock()
	}(m, args.WorkerId)

	fmt.Println("After assign task")
	m.Print()
	fmt.Println("......")
	return nil
}

func (m *Master) WorkerComplete(args *CompleteArgs, reply *CompleteReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	worker_id := args.WorkerId
	task_id := args.TaskId
	task_type := args.TaskType
	//m.Print()
	if t,ok := m.Wip_tasks[worker_id]; ok == true && t.Tid == task_id && t.Type == task_type{
		delete(m.Wip_tasks, worker_id)
		if m.Phase == "Map" {
			for _,f := range(args.Results) {
				fmt.Println(f)
				reduce_id,err := strconv.Atoi(strings.Split(f, "-")[2])
				fmt.Println(reduce_id)
				if err == nil {
					m.Reduce_inputs[reduce_id] =append(m.Reduce_inputs[reduce_id], f)
				}
			}
			if len(m.Unstarted_tasks) == 0 && len(m.Wip_tasks) == 0 {
				m.Phase = "Reduce"
				m.Unstarted_tasks = make([]Task, len(m.Reduce_inputs))
				for i, files := range(m.Reduce_inputs) {
					m.Unstarted_tasks[i] = Task{i, "Reduce", files}
					m.Reduce_inputs[i] = nil
				}
			}
		}
	}
	fmt.Println("After Complete")
	m.Print()
	fmt.Println("......")
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Phase == "Reduce" && len(m.Unstarted_tasks) == 0 && len(m.Wip_tasks) == 0 {
		return true
	}

	return false;
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}


	// Your code here.
	m.Phase = "Map"
	m.Unstarted_tasks = make([]Task, len(files))
	for i, f := range(files) {
		m.Unstarted_tasks[i] = Task{i, "Map", []string{f}}
	}
	m.Wip_tasks = make(map[int]Task)
	m.Reduce_inputs = make([][]string, nReduce)


	m.server()
	return &m
}
