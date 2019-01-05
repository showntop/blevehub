package main

import (
	ecsv "encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
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

// var maxprocs = flag.Int("maxprocs", 16, "GOMAXPROCS")
var indexPath = flag.String("index", "indexes", "index storage path")
var docsPath = flag.String("docs", "./data/aaaa.csv", "path to docs file")
var csv = flag.Bool("csv", true, "summary CSV output")

func main() {
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	flag.Parse()

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

	log.Println("start")
	stime := time.Now()
	myhub, err := hub.NewHub("./indexes", "testhub", 5)
	if err != nil {
		log.Println(err)
	}

	myhub.IndexBatch(docs)
	log.Println("end index")
	// myhub.ForTest()
	log.Println("build index use ", time.Now().Sub(stime))

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
		log.Println("%s used ", req.RequestURI, time.Now().Sub(stime))
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
