package hub

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
)

type Document interface {
	ID() string
	Data() interface{}
	Source() []byte
}

type Hub struct {
	path string // Path to storage

	shards []*Shard         // Index shards i.e. bleve indexes
	alias  bleve.IndexAlias // All bleve indexes as one reference, for search
}

func (h *Hub) Sharding(docId string) *Shard {
	hasher := fnv.New32a()
	hasher.Write([]byte(docId))
	v := hasher.Sum32() % uint32(len(h.shards))
	return h.shards[v]
}

func (h *Hub) IndexBatch(documents []Document) error {
	var wg sync.WaitGroup
	shardBatches := make(map[*Shard][]Document, 0)
	for _, d := range documents {
		shard := h.Sharding(d.ID())
		shardBatches[shard] = append(shardBatches[shard], d)
	}

	// Index each batch in parallel.
	for shard, batch := range shardBatches {
		wg.Add(1)
		go func(s *Shard, b []Document) {
			defer wg.Done()
			s.IndexBatch(b)
		}(shard, batch)
	}
	wg.Wait()
	return nil
}

func (h *Hub) Count() (uint64, error) {
	return h.alias.DocCount()
}

func (h *Hub) Search(rawReq []byte) (*bleve.SearchResult, error) {
	var searchRequest bleve.SearchRequest
	err := json.Unmarshal(rawReq, &searchRequest)
	if err != nil {
		return nil, fmt.Errorf("error parsing query: %v", err)
	}

	if srqv, ok := searchRequest.Query.(query.ValidatableQuery); ok {
		err = srqv.Validate()
		if err != nil {
			return nil, fmt.Errorf("error validating query: %v", err)
		}
	}

	// execute the query
	searchResponse, err := h.alias.Search(&searchRequest)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %v", err)
	}
	return searchResponse, nil
	// docIDs := make(DocIDs, 0, len(searchResults.Hits))
	// for _, d := range searchResults.Hits {
	// 	docIDs = append(docIDs, DocID(d.ID))
	// }
	// sort.Sort(docIDs)
	// return docIDs, nil
}

func (h *Hub) ForTest() {
	fields, _ := h.shards[0].idx.Fields()
	fmt.Println(fields)

	dict, _ := h.shards[0].idx.FieldDict("Body")
	// x, _ := dict.Next()
	for {
		x, err := dict.Next()
		if err != nil {
			fmt.Println(err)
			break
		}
		_ = x
		fmt.Printf("%+v", x)
	}

}

func NewHub(path string, name string, numShards int) (*Hub, error) {
	indexName := name
	indexPath := filepath.Join(path, indexName)

	// if numShards > maxShardCount {
	// 	return nil, fmt.Errorf("requested shard count exceeds maximum of %d", maxShardCount)
	// }
	os.RemoveAll(indexPath)
	// Create the directory for the index, if it doesn't already exist.
	if _, err := os.Stat(indexPath); err != nil && os.IsExist(err) {
		return nil, fmt.Errorf("index already exists at %s", indexPath)
	}
	if err := os.MkdirAll(indexPath, 0755); err != nil {
		return nil, err
	}

	// Create the shards.
	shards := make([]*Shard, 0, numShards)
	for n := 0; n < numShards; n++ {
		s := NewShard(filepath.Join(indexPath, fmt.Sprintf("%04d", n)))
		if err := s.Open(); err != nil {
			return nil, err
		}
		shards = append(shards, s)
	}

	// Create alias for searching.
	alias := bleve.NewIndexAlias()
	for _, s := range shards {
		alias.Add(s.idx)
	}

	return &Hub{
		path:   indexPath,
		shards: shards,
		alias:  alias,
	}, nil
}

// OpenHub opens an existing index, at the given path.
func OpenHub(path string) (*Hub, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to access hub at %s", path)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("hub %s path is not a directory", path)
	}

	// Open the shards.
	names, err := listShards(path)
	if err != nil {
		return nil, err
	}

	shards := make([]*Shard, 0)
	for _, name := range names {
		s := NewShard(filepath.Join(path, name))
		if err := s.Open(); err != nil {
			return nil, fmt.Errorf("shard open fail: %s", err.Error())
		}
		shards = append(shards, s)
	}

	// Create alias for searching.
	alias := bleve.NewIndexAlias()
	for _, s := range shards {
		alias.Add(s.idx)
	}

	// Index is ready to go.
	return &Hub{
		path:   path,
		shards: shards,
		alias:  alias,
	}, nil
}

func listShards(path string) ([]string, error) {
	// Get an index directory listing.
	d, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer d.Close()

	fis, err := d.Readdir(0)
	if err != nil {
		return nil, err
	}

	// Get the shard names in alphabetical order.
	var names []string
	for _, fi := range fis {
		if !fi.IsDir() || strings.HasPrefix(fi.Name(), ".") {
			continue
		}
		names = append(names, fi.Name())
	}
	sort.Strings(names)
	return names, nil
}
