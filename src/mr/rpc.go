package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

type TaskStatus int

const (
	IDLE TaskStatus = iota + 1
	MAP_IN_PROCESS
	MAP_COMPLETED
	REDUCE_IDLE
	REDUCE_IN_PROCESS
	REDUCE_COMPLETED
	DONE
)

type Task struct {
	WorkerID             int
	Filename             string // 仅用于map任务，reduce任务下置为空串
	Status               TaskStatus
	NMap                 int      // 对于map任务来说，指代的是map任务的编号，对reduce任务来说，置为-1
	NReduce              int      // 对于map任务来说，指代的是reduce任务数量，对于reduce任务来说，指代的是reduce任务的编号
	IntermediateFileList []string // 对于map和reduce任务来说，中间文件列表的含义不同，map是行文件列表，reduce是列文件列表
	StartTime            time.Time
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
