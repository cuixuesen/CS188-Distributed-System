package mapreduce

import "container/list"
import "fmt"
import "sync"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}


//assign map and reduce jobs to workers through RPC in parallel
func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	var nmapChannel    = make(chan string)
  var nreduceChannel = make(chan string)
  var mutex = &sync.Mutex{}
	// start to map
	for i:=0; i < mr.nMap; i++ {
     go func(j int){
     		for {
			 			var availableWorker string
						select{
							case availableWorker = <-mr.myChannel:
							case availableWorker = <-mr.registerChannel:
								mutex.Lock()
								mr.Workers[availableWorker]=&WorkerInfo{availableWorker}
								mutex.Unlock()

						}
						//RPC to remote work to start mapping
						if call(availableWorker, "Worker.DoJob", &DoJobArgs{mr.file, Map, j, mr.nReduce}, &DoJobReply{}) {
							nmapChannel <- "map"
							mr.myChannel <-availableWorker
						  return
						}
				}
		}(i)
	}
	// wait worker finish mapping
	for i:=0; i<mr.nMap;i++ { <-nmapChannel }

	// start to reduce
	for i:=0; i < mr.nReduce; i++ {
     go func(j int){
     		for {
			 			var availableWorker string
						select{
							case availableWorker = <-mr.myChannel:
							case availableWorker = <-mr.registerChannel:
							   mutex.Lock()
								 mr.Workers[availableWorker]=&WorkerInfo{availableWorker}
								 mutex.Unlock()
						}
						//RPC to remote work to start reducing
						if call(availableWorker, "Worker.DoJob", &DoJobArgs{mr.file, Reduce, j, mr.nMap}, &DoJobReply{}) {
							nreduceChannel <- "reduce"
							mr.myChannel <-availableWorker
							return
						}
				}
		}(i)
	}
	//wait worker finish reducing
	for i:=0; i<mr.nReduce;i++ { <-nreduceChannel }

	return mr.KillWorkers()
}
