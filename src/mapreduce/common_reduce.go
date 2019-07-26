package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	var f *os.File
	var dec *json.Decoder
	var err error
	var kv KeyValue
	// 关键：使用map来合并具有相同key的KeyValue。
	// 我们可以不用直接使用结构体KeyValue，多个具有相同key的KeyValue，其中真正的信息只有一个key和多个value。
	m := make(map[string][]string)
	// 打开nMap个map tasks为该reduce task计算出的nMap个文件，以及新建nMap个decoder。
	for i := 0; i < nMap; i++ {
		// func reduceName(jobName string, mapTask int, reduceTask int) string
		f, err = os.Open(reduceName(jobName, i, reduceTask))
		if err != nil {
			log.Fatal(err)
		}
		dec = json.NewDecoder(f)
		for dec.More() {
			dec.Decode(&kv)
			m[kv.Key] = append(m[kv.Key], kv.Value)
		}
		f.Close()
	}

	f, err = os.Create(outFile)
	defer f.Close()
	enc := json.NewEncoder(f)
	if err != nil {
		log.Fatal(err)
	}

	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		err := enc.Encode(&KeyValue{Key: k, Value: reduceF(k, m[k])})
		if err != nil {
			log.Fatal(err)
		}
	}
}
