package hub

import (
	"fmt"
	"os"

	"github.com/blevesearch/bleve"

	"github.com/blevesearch/bleve/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/analysis/tokenizer/regexp"
	"github.com/blevesearch/bleve/mapping"
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
		mapping, err := buildIndexMapping()
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
	batch := s.idx.NewBatch()

	for _, d := range documents {
		if err := batch.Index(string(d.ID()), d.Data()); err != nil {
			return err // XXX return errors en-masse
		}
		batch.SetInternal([]byte(d.ID()), d.Source())
	}
	return s.idx.Batch(batch)
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