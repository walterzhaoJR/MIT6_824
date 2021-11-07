package mr

import "C"
import (
	"errors"
	"fmt"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

var mutex sync.Mutex

type Coordinator struct {
	// Your definitions here.
	state			int // init(0), running(1), finish(2)
	mapTasks 		[]Task
	reduceTasks 	[]Task
	nReduce			int // reduce job num
	nMapTaskNum		int // map job num
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

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	var err error
	reply.Id = -1


	mutex.Lock()
	if 0 == c.state {
		log.Println("Coordinator will init")
		err = errors.New("Coordinator will init\n")
	} else if 1 == c.state {
		// log.Println("Coordinator is running")
	} else if 2 == c.state {
		log.Println("Coordinator is finish")
		err = errors.New("Coordinator is finish\n")
	}

	if 0 == args.TaskType { // map task
		if 0 != c.nMapTaskNum {
			for index, task := range c.mapTasks {
				if task.State == 1 || task.State == 2 { // task is running or finish
					log.Printf("index :%v task:%v is rnunng or finish", index, task)
					continue
				} else { // task is init
					// task.State = 1 // running
					c.mapTasks[index].State = 1 // running
					reply.Id = task.Id
					reply.TaskType = task.TaskType
					reply.FileName = task.InputFileName
					log.Printf("task:%v reply.task:%v", task, reply)
					break
				}
			}

			// all task is running return wait
			if -1 == reply.Id {
				log.Println("all map task is running")
				err = errors.New("all map task is running") // all map task is running
			}
		} else { // all task finish
			log.Println("all map task finish")
			err = errors.New("all map task finish")
		}
	} else if 1 == args.TaskType { // reduce task
		// err = 9999
	} else {
		// err = 9999
		// err = errors.New("unknown task type")
	}
	mutex.Unlock()

	return err
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
	mutex.Lock()
	c.state = 0 // init
	c.nReduce = nReduce
	c.nMapTaskNum = 0

	for i, fileName := range files {
		fmt.Println(i, fileName)
		task := Task {
			Id: i,
			State: 0,
			InputFileName: fileName,
			TaskType: 0,
		}

		c.mapTasks = append(c.mapTasks, task)
		c.nMapTaskNum++
	}

	fmt.Println(c)
	c.state = 1 // running
	mutex.Unlock()

	c.server()
	return &c
}
