package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

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

func checkFileIsExist(filename string) bool {
	var exist = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

var workerID int

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	var access bool
	workerID, access = CallRegistWorker()
	if access != true {
		return
	}

	for {
		task := CallFetchTask()
		if task.Status == MAP_IN_PROCESS {
			handleMapTask(mapf, &task)
		}
		if task.Status == REDUCE_IN_PROCESS {
			handleReduceTask(reducef, &task)
		}
		if task.Status == DONE {
			break
		}
	}
	// fmt.Printf("worker %v exit", workerID)
}

func handleMapTask(mapf func(string, string) []KeyValue, task *Task) {

	intermediate := []KeyValue{}
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	kva := mapf(task.Filename, string(content))
	intermediate = append(intermediate, kva...)

	ia := make(map[int][]KeyValue)

	for _, kv := range intermediate {
		slot := ihash(kv.Key) % task.NReduce
		ia[slot] = append(ia[slot], kv)
	}

	for key, value := range ia {
		filename := "mr-" + strconv.Itoa(task.NMap) + "-" + strconv.Itoa(key)

		var err error
		var f io.Writer
		f, err = os.Create(filename) //创建文件

		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		task.IntermediateFileList = append(task.IntermediateFileList, filename)
		for _, kv := range value {
			enc := json.NewEncoder(f)
			enc.Encode(kv)
		}
	}
	CallAckMapCompleted(task)
}

func handleReduceTask(reducef func(string, []string) string, task *Task) {

	var kva []KeyValue
	for _, filename := range task.IntermediateFileList {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", task.Filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	tempFileName := fmt.Sprintf("mr-tmp-%v", task.NReduce)
	ofile, err := os.Create(tempFileName)
	if err != nil {
		log.Fatalf("cannot create temp file: %v", tempFileName)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
	outFileName := fmt.Sprintf("mr-out-%v", task.NReduce)
	os.Rename(tempFileName, outFileName)
	if err != nil {
		log.Fatalf("error: %v, cannot rename %v", err, tempFileName)
	}

	CallAckReduceCompleted(task)
}

func CallRegistWorker() (int, bool) {
	args := 0

	reply := 0

	ret := call("Master.RegistWorker", &args, &reply)

	return reply, ret
}

func CallAckMapCompleted(task *Task) {
	args := task

	reply := 0

	call("Master.AckMapCompleted", &args, &reply)

}

func CallAckReduceCompleted(task *Task) {
	args := task

	reply := 0

	call("Master.AckReduceCompleted", &args, &reply)

}

func CallFetchTask() Task {

	args := workerID

	reply := Task{}

	call("Master.FetchTask", &args, &reply)

	// fmt.Println(reply)
	return reply
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

	// fmt.Println(err)
	return false
}
