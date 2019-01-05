package hub

import (
	_ "blevehub/analysis"
	"fmt"
	"os"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/analysis/tokenizer/regexp"
	"github.com/blevesearch/bleve/mapping"
	"github.com/yanyiwu/gojieba"
)

type Shard struct {
	path string
	idx  bleve.Index
}

func NewShard(path string) *Shard {
	return &Shard{
		path: path,
	}
}

func (s *Shard) Open() error {
	_, err := os.Stat(s.path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to check existence of shard")
	} else if os.IsNotExist(err) {
		mapping, err := mapping3()
		if err != nil {
			return err
		}
		s.idx, err = bleve.New(s.path, mapping)
		if err != nil {
			return fmt.Errorf("bleve new: %s", err.Error())
		}
	} else {
		s.idx, err = bleve.Open(s.path)
		if err != nil {
			return fmt.Errorf("bleve open: %s", err.Error())
		}
	}
	return nil
}

func (s *Shard) Close() error {
	return s.idx.Close()
}

func (s *Shard) Index(document Document) error {
	// s.idx
	return nil
}

func (s *Shard) IndexBatch(documents []Document) error {

	batchSize := 500
	// batchNum := len(documents) / batchSize
	// if len(documents)%batchSize != 0 {
	// 	batchNum += 1
	// }

	batch := s.idx.NewBatch()
	for i, d := range documents {
		if i%batchSize == 0 || i == len(documents)-1 {
			if err := s.idx.Batch(batch); err != nil {
				return err
			}
			batch = s.idx.NewBatch()
		} else {
			if err := batch.Index(string(d.ID()), d.Data()); err != nil {
				return err // XXX return errors en-masse
			}
		}
		// batch.SetInternal([]byte(d.ID()), d.Source())
	}
	return nil
}

func buildIndexMapping() (*mapping.IndexMappingImpl, error) {
	var err error

	// Create the index mapping, configure the analyzer, and set as default.
	indexMapping := bleve.NewIndexMapping()
	err = indexMapping.AddCustomTokenizer("ekanite_tk",
		map[string]interface{}{
			"regexp": `[^\W_]+`,
			"type":   regexp.Name,
		})
	if err != nil {
		return nil, err
	}

	err = indexMapping.AddCustomAnalyzer("ekanite",
		map[string]interface{}{
			"type":          custom.Name,
			"char_filters":  []interface{}{},
			"tokenizer":     `ekanite_tk`,
			"token_filters": []interface{}{`to_lower`},
		})
	if err != nil {
		return nil, err
	}
	indexMapping.DefaultAnalyzer = "ekanite"

	// Create field-specific mappings.

	simpleJustIndexed := bleve.NewTextFieldMapping()
	simpleJustIndexed.Store = false
	simpleJustIndexed.IncludeInAll = true // XXX Move to false when using AST
	simpleJustIndexed.IncludeTermVectors = false

	timeJustIndexed := bleve.NewDateTimeFieldMapping()
	timeJustIndexed.Store = false
	timeJustIndexed.IncludeInAll = false
	timeJustIndexed.IncludeTermVectors = false

	articleMapping := bleve.NewDocumentMapping()

	// Connect field mappings to fields.
	articleMapping.AddFieldMappingsAt("Message", simpleJustIndexed)
	articleMapping.AddFieldMappingsAt("ReferenceTime", timeJustIndexed)
	articleMapping.AddFieldMappingsAt("ReceptionTime", timeJustIndexed)

	// Tell the index about field mappings.
	indexMapping.DefaultMapping = articleMapping

	return indexMapping, nil
}

func mapping2() (*mapping.IndexMappingImpl, error) {
	// a generic reusable mapping for english text
	standardJustIndexed := bleve.NewTextFieldMapping()
	standardJustIndexed.Store = false
	standardJustIndexed.IncludeInAll = false
	standardJustIndexed.IncludeTermVectors = false
	standardJustIndexed.Analyzer = "sego"

	articleMapping := bleve.NewDocumentMapping()

	// body
	articleMapping.AddFieldMappingsAt("Body", standardJustIndexed)

	indexMapping := bleve.NewIndexMapping()
	indexMapping.DefaultMapping = articleMapping
	indexMapping.DefaultAnalyzer = "sego"

	err := indexMapping.AddCustomTokenizer("sego",
		map[string]interface{}{
			"dictpath": "./dicts/dictionary.txt",
			"type":     "sego",
		},
	)
	if err != nil {
		return nil, err
	}
	err = indexMapping.AddCustomAnalyzer("sego",
		map[string]interface{}{
			"type":      "sego",
			"tokenizer": "sego",
		},
	)
	if err != nil {
		return nil, err
	}

	return indexMapping, nil
}

func mapping3() (*mapping.IndexMappingImpl, error) {
	// a generic reusable mapping for english text
	standardJustIndexed := bleve.NewTextFieldMapping()
	standardJustIndexed.Store = false
	standardJustIndexed.IncludeInAll = false
	standardJustIndexed.IncludeTermVectors = false
	standardJustIndexed.Analyzer = "gojieba"

	articleMapping := bleve.NewDocumentMapping()

	// body
	articleMapping.AddFieldMappingsAt("Body", standardJustIndexed)

	indexMapping := bleve.NewIndexMapping()
	indexMapping.DefaultMapping = articleMapping
	indexMapping.DefaultAnalyzer = "gojieba"

	err := indexMapping.AddCustomTokenizer("gojieba",
		map[string]interface{}{
			"dictpath":     gojieba.DICT_PATH,
			"hmmpath":      gojieba.HMM_PATH,
			"userdictpath": gojieba.USER_DICT_PATH,
			"idf":          gojieba.IDF_PATH,
			"stop_words":   gojieba.STOP_WORDS_PATH,
			"type":         "gojieba",
		},
	)
	if err != nil {
		panic(err)
	}
	err = indexMapping.AddCustomAnalyzer("gojieba",
		map[string]interface{}{
			"type":      "gojieba",
			"tokenizer": "gojieba",
		},
	)
	if err != nil {
		panic(err)
	}

	return indexMapping, nil
}
