package main

import (
	ecsv "encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime/pprof"
	// _ "net/http/pprof"
	"os"
	"runtime"
	"time"

	"blevehub/hub"
)

type article struct {
	title string
	Body  string
}

type mydoc struct {
	id   string
	data article
}

func (m *mydoc) ID() string {
	return m.id
}

func (m *mydoc) Data() interface{} {
	return m.data
}

func (m *mydoc) Source() []byte {
	return nil
}

var addr = flag.String("addr", ":8094", "server addr")
var batchSize = flag.Int("batchSize", 100, "batch size for indexing")
var nShards = flag.Int("shards", 2, "number of indexing shards")
var cpuprofile = flag.String("cpuprofile", "./log/cupprofile", "write cpu profile to file")
var memprofile = flag.String("memprofile", "./log/memprofile", "write mem profile to file")

// var maxprocs = flag.Int("maxprocs", 16, "GOMAXPROCS")
var indexPath = flag.String("index", "indexes", "index storage path")
var docsPath = flag.String("docs", "./data/aaaa.csv", "path to docs file")

var csv = flag.Bool("csv", true, "summary CSV output")

func main() {
	log.SetOutput(os.Stdout)
	// go func() {
	// 	log.Println(http.ListenAndServe(":6060", nil))
	// }()

	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	f, err := os.Open(*docsPath)
	if err != nil {
		fmt.Printf("failed to open docs file: %s\n", err.Error())
		os.Exit(1)
	}

	reader := ecsv.NewReader(f)

	rows, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("failed to read docs file: %s\n", err.Error())
		os.Exit(1)
	}

	var docs = make([]hub.Document, 0, len(rows)-1)
	for i := 0; i < len(rows); i++ {
		ar := article{
			title: rows[i][2],
			Body:  rows[i][3],
		}
		docs = append(docs, &mydoc{
			id:   fmt.Sprint(i),
			data: ar,
		})
	}

	log.Printf("start with GOMAXPROCS:%d and %d shards(batch size: %d), total docs:%d\r\n", runtime.GOMAXPROCS(-1), *nShards, *batchSize, len(docs))

	stime1 := time.Now()
	myhub, err := hub.NewHub(*indexPath, "testhub", *nShards, *batchSize)
	if err != nil {
		log.Println(err)
	}
	log.Printf("new hub use %s\r\n", time.Now().Sub(stime1))
	stime := time.Now()

	myhub.IndexBatch(docs)
	// myhub.ForTest()
	pprof.StopCPUProfile()
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
	x, _ := myhub.Count()
	log.Printf("build %d index finish, use %s \r\n", x, time.Now().Sub(stime))

	http.HandleFunc("/api/search", xxx(myhub))
	http.HandleFunc("/api/count", xxx2(myhub))
	http.ListenAndServe(*addr, nil)

}

func xxx2(h *hub.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		stime := time.Now()
		x, err := h.Count()
		if err != nil {
			http.Error(w, "error", 500)
			return
		}
		w.Write([]byte(fmt.Sprint(x)))
		log.Printf("%s used %s \r\n", req.RequestURI, time.Now().Sub(stime))
	}
}

func xxx(h *hub.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		stime := time.Now()
		rawReq, err := ioutil.ReadAll(req.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("error reading request body: %v", err), 400)
			return
		}
		resp, err := h.Search(rawReq)
		if err != nil {
			http.Error(w, fmt.Sprintf("error reading request body: %v", err), 400)
			return
		}

		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Content-type", "application/json")

		e := json.NewEncoder(w)
		if err := e.Encode(resp); err != nil {
			panic(err)
		}
		log.Println("used ", time.Now().Sub(stime))
	}
}
