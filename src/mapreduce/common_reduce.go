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

  debug("reduce outFile %s\n", outFile)
	file, err := os.Create(outFile);
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer file.Close()
  enc := json.NewEncoder(file)


  i := 0
  for {
    if i >= nMap {
      break
    }
    inFile := reduceName(jobName, reduceTask, i)
    debug("reduce inFile %s\n", inFile)



    kvs := make(map[string][]string)

    in, err := os.Open(inFile)
    if err != nil {
      log.Fatal("Reduce: ", err)
    }
    defer in.Close()
    dec := json.NewDecoder(in)
    for {
      var kv KeyValue
      err = dec.Decode(&kv)
      if err != nil {
        break
      }
      kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
    }
    var keys []string
    for k := range kvs {
      keys = append(keys, k)
    }
    sort.Strings(keys)

    for i := range keys {
      k := keys[i]
      res := reduceF(k, kvs[k])
      kv := KeyValue{k, res}
      err := enc.Encode(&kv)
      if err != nil {
        log.Fatal("check: ", err)
      }
    }
    i++
  }
}

// 15-     debug("Merge phase")
// 16-     kvs := make(map[string]string)
// 17-     for i := 0; i < mr.nReduce; i++ {
// 18-             p := mergeName(mr.jobName, i)
// 19-             fmt.Printf("Merge: read %s\n", p)
// 20-             file, err := os.Open(p)
// 21-             if err != nil {
// 22-                     log.Fatal("Merge: ", err)
// 23-             }
// 24:             dec := json.NewDecoder(file)
// 25-             for {
// 26-                     var kv KeyValue
// 27-                     err = dec.Decode(&kv)
// 28-                     if err != nil {
// 29-                             break
// 30-                     }
// 31-                     kvs[kv.Key] = kv.Value
// 32-             }
// 33-             file.Close()
// 34-     }
// 35-     var keys []string
// 36-     for k := range kvs {
// 37-             keys = append(keys, k)
// 38-     }
// 39-     sort.Strings(keys)
// 40-
