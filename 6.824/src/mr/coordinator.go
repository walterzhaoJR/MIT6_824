package mr

import (
	"fmt"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	state			int // init(0), running(1), finish(2)
	mapTasks 		[]Task
	reduceTasks 	[]Task
	nReduce			int // reduce job num
}

type Task struct {
	Id					int
	State				int // init(0), running(1), finish(2)
	InputFileName		string
	OutputFileName		string
	TaskType			int // map(0), reduce(1)
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
	} else {
		log.Println("listen success")
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.state = 0 // init
	c.nReduce = nReduce

	for i, fileName := range files {
		fmt.Println(i, fileName)
		task := Task {
			Id: i,
			State: 0,
			InputFileName: fileName,
			TaskType: 0,
		}

		c.mapTasks = append(c.mapTasks, task)
	}

	fmt.Println(c)

	c.server()
	return &c
}