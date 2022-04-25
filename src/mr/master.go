package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Phase int

const (
	MapPhase Phase = iota + 1
	ReducePhase
	MapReduceDone
)

type Master struct {
	// Your definitions here.
	condMap            *sync.Cond // 指示当前是否存在就绪的map任务
	condReduce         *sync.Cond // 指示当前是否存在就绪的reduce任务
	condMapComplete    *sync.Cond // 指示是否完成了所有的map任务
	condReduceComplete *sync.Cond // 指示是否完成了所有的reduce任务
	mutex              sync.Mutex

	mapTasks    []Task
	reduceTasks []Task

	nReduce int

	intermediateFileList []string

	phase Phase

	workSeq int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) FetchTask(workerID *int, reply *Task) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	var task *Task
	if m.phase == MapPhase {
		task = m.fetchMapTask(*workerID)
		if task == nil {
			return errors.New("Map任务已经结束嘞")
		}
	} else if m.phase == ReducePhase {
		task = m.fetchReduceTask(*workerID)
		if task == nil {
			return errors.New("Reduce任务已经结束嘞")
		}
	} else if m.phase == MapReduceDone {
		task = &Task{-1, "", DONE, -1, -1, nil, time.Now()}
	}
	// fmt.Println(task)
	*reply = *task
	return nil
}

func (m *Master) fetchMapTask(workerID int) *Task {
	var index int
	for index = m.getIdleMapTaskIndex(); index == -1; {
		m.condMap.Wait()
		if m.phase != MapPhase {
			return nil
		}
		index = m.getIdleMapTaskIndex()
	}
	task := &m.mapTasks[index]
	task.Status = MAP_IN_PROCESS
	task.StartTime = time.Now()
	task.WorkerID = workerID

	go func(task *Task) { // 设置十秒超时
		time.Sleep(time.Second * 10)
		m.mutex.Lock()
		if task.Status != MAP_COMPLETED {
			// fmt.Println(fmt.Sprintf("map task timeout: %v", task.Filename))
			task.Status = IDLE
			m.condMap.Broadcast()
		}
		m.mutex.Unlock()
	}(task)
	return task
}

func (m *Master) fetchReduceTask(workerID int) *Task {
	var index int
	for index = m.getIdleReduceTaskIndex(); index == -1; {
		m.condReduce.Wait()
		if m.phase != ReducePhase {
			return nil
		}
		index = m.getIdleReduceTaskIndex()
	}
	task := &m.reduceTasks[index]
	task.Status = REDUCE_IN_PROCESS
	task.StartTime = time.Now()
	task.WorkerID = workerID

	go func(task *Task) { // 设置十秒超时
		time.Sleep(time.Second * 10)
		m.mutex.Lock()
		if task.Status != REDUCE_COMPLETED {
			// fmt.Println(fmt.Sprintf("reduce task timeout: %v", task.NReduce))
			task.Status = REDUCE_IDLE
			m.condReduce.Broadcast()
		}
		m.mutex.Unlock()
	}(task)
	return task
}

func (m *Master) getIdleMapTaskIndex() int {
	for index, temp := range m.mapTasks {
		if temp.Status == IDLE {
			return index
		}
	}
	return -1
}

func (m *Master) getIdleReduceTaskIndex() int {
	for index, temp := range m.reduceTasks {
		if temp.Status == REDUCE_IDLE {
			return index
		}
	}
	return -1
}

func (m *Master) AckMapCompleted(args *Task, reply *int) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for i, task := range m.mapTasks {
		if task.Filename == args.Filename {
			if time.Since(task.StartTime) > time.Second*10 { // 如果超时的话，master会认为worker崩溃了，并re-issue当前任务，所以后续到达的超时结果就丢弃了
				return errors.New("当前task已经完成了")
			}
			m.mapTasks[i].Status = MAP_COMPLETED
			m.intermediateFileList = append(m.intermediateFileList, args.IntermediateFileList...)
			m.condMapComplete.Broadcast() // 唤醒checkMapComplete线程，检测是否全部完成
			break
		}
	}
	return nil
}

func (m *Master) AckReduceCompleted(args *Task, reply *int) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if time.Since(args.StartTime) > time.Second*10 { // 如果超时的话，master会认为worker崩溃了，并re-issue当前任务，所以后续到达的超时结果就丢弃了
		return errors.New(fmt.Sprintf("当前reduce task %v已经完成了", args.NReduce))
	}
	m.reduceTasks[args.NReduce].Status = REDUCE_COMPLETED
	m.condReduceComplete.Broadcast() // 唤醒checkMapComplete线程，检测是否全部完成

	return nil
}

func (m *Master) checkAllMapComplete() {

	m.condMapComplete = sync.NewCond(&m.mutex)
	m.mutex.Lock()

	flag := false

	for flag != true {
		flag = true
		m.condMapComplete.Wait()
		for _, task := range m.mapTasks { // 是否全部完成
			if task.Status != MAP_COMPLETED {
				flag = false
				break
			}
		}
	}
	m.phase = ReducePhase
	m.createReduceTask()

	m.condMap.Broadcast() // 通知等待在map任务上的RPC无需等待了
	m.mutex.Unlock()
}

func (m *Master) checkAllReduceComplete() {
	m.condReduceComplete = sync.NewCond(&m.mutex)
	m.mutex.Lock()

	flag := false
	for flag != true {
		flag = true
		m.condReduceComplete.Wait()
		for _, task := range m.reduceTasks { // 是否全部完成
			if task.Status != REDUCE_COMPLETED {
				flag = false
				break
			}
		}
	}
	m.condReduce.Broadcast()
	m.phase = MapReduceDone
	m.mutex.Unlock()
}

func (m *Master) createMapTask(files []string, nReduce int) {
	m.condMap = sync.NewCond(&m.mutex)
	m.phase = MapPhase

	for i, file := range files {
		task := Task{-1, file, IDLE, i, nReduce, nil, time.Now()}
		m.mapTasks = append(m.mapTasks, task)
	}
	go m.checkAllMapComplete() // 检测map task是否全部完成
}

func (m *Master) createReduceTask() {
	m.condReduce = sync.NewCond(&m.mutex)
	for i := 0; i < m.nReduce; i++ {
		var reduceTask Task
		reduceTask.Filename = ""
		reduceTask.Status = REDUCE_IDLE
		reduceTask.NMap = -1
		reduceTask.NReduce = i
		m.reduceTasks = append(m.reduceTasks, reduceTask)
	}
	for _, filename := range m.intermediateFileList {
		strs := strings.Split(filename, "-")
		nReduce, _ := strconv.Atoi(strs[len(strs)-1])
		m.reduceTasks[nReduce].IntermediateFileList = append(m.reduceTasks[nReduce].IntermediateFileList, filename)
	}
	go m.checkAllReduceComplete() // 检测reduce task是否全部完成
}

func (m *Master) RegistWorker(args *int, reply *int) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	*reply = m.workSeq
	m.workSeq = m.workSeq + 1

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
	ret := false

	// Your code here.
	m.mutex.Lock()
	if m.phase == MapReduceDone {
		// fmt.Println("Done")
		ret = true
	}
	m.mutex.Unlock()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.

	m.nReduce = nReduce
	m.createMapTask(files, nReduce)

	m.server()
	return &m
}
