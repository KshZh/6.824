package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var wg sync.WaitGroup
	// for worker := range registerChan {
	// 	// 有worker刚启动或刚完成了计算，可以接着接受新的计算任务。
	// 	ntasks--
	// 	wg.Add(1)
	// 	go func(i int, worker string) {
	// 		args := DoTaskArgs{JobName: jobName, File: mapFiles[i], Phase: phase, TaskNumber: i, NumOtherPhase: n_other}
	// 		// 如果rpc不成功就继续尝试。
	// 		for !call(worker, "Worker.DoTask", args, nil) {
	// 		}
	// 		go func() {
	// 			registerChan <- worker
	// 		}() // 另开线程，避免阻塞。
	// 		wg.Done()
	// 	}(ntasks, worker)
	// 	if ntasks == 0 {
	// 		break // ntasks个文件全处理完了。
	// 	}
	// }
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			arg := DoTaskArgs{JobName: jobName, File: mapFiles[i], Phase: phase, TaskNumber: i, NumOtherPhase: n_other}
			for {
				w := <-registerChan // 获取一个可以接受计算任务的worker。
				if call(w, "Worker.DoTask", arg, nil) {
					go func() {
						registerChan <- w
					}()
					return
				}
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)

	// 上面第一种写法的问题在于，rpc失败后是对同一个worker重试，但往往rpc失败是worker网络不连通或crash了，
	// 这时应该换一个worker才是。
	// 同时第二种写法的思路清晰很多，也就是对ntasks个task，**每个task一个rpc线程**，且在同一个线程中，
	// rpc失败后，可以很方便地获取到另一个worker进行rpc。
}
