package eskit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/lytics/eskit"
	elastigo "github.com/mattbaird/elastigo/lib"
)

var _ eskit.DocScroller = (*Elastic5Scroll)(nil)

type ScrollHandler interface {
	ElastigoScroller
	DoCommand(string, string, map[string]interface{}, interface{}) ([]byte, error)
}

type ElastigoScroller interface {
	Search(string, string, map[string]interface{}, interface{}) (elastigo.SearchResult, error)
	Scroll(args map[string]interface{}, scroll_id string) (elastigo.SearchResult, error)
}

type ScrollSettings struct {
	Hosts    []string
	Port     string
	Index    string
	Pagesize int64
	Timeout  string
}

// Elastic5Scroll implements the DocScroller interface for Opening and Iterating
// over an Elasticsearch Index's data set.
//
// Accepts a ScrollSettings struct which is used to create the client connections
// and defines runtime variables.
//
// Fortunately the Scroll API didn't change between 2x and 5x so this functionality
// works fine for both!
type Elastic5Scroll struct {
	// Configuration
	ss ScrollSettings

	// Client
	conn ScrollHandler

	// State
	scrollId  string
	scrollmux sync.Mutex
	scrollIds map[string]struct{}
}

//func NewElastic5Scroll(ips []string, index string, scrolltimeout string, pagesize int64) *Elastic5Scroll {
func NewElastic5Scroll(ss ScrollSettings) *Elastic5Scroll {
	c := elastigo.NewConn()
	c.SetHosts(ss.Hosts)
	c.SetPort(ss.Port)

	return &Elastic5Scroll{
		ss:        ss,
		conn:      c,
		scrollIds: map[string]struct{}{},
	}
}

// Open queries Elasticsearch to create a scroll opeartion and returns the documents
// if successful. Parses the SearchResults and stores the scroll identifier.
func (e *Elastic5Scroll) Open(query map[string]interface{}) ([]*eskit.Doc, error) {
	args := map[string]interface{}{"scroll": e.ss.Timeout}
	qb, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	sr, err := e.conn.Search(e.ss.Index, "", args, string(qb))
	if err != nil {
		return nil, err
	}
	e.setScrollID(sr.ScrollId)

	return resultDocs(sr), nil
}

// Scroll over the next range of documents from the identifier parameter
func (e *Elastic5Scroll) Scroll(id string) ([]*eskit.Doc, error) {
	args := map[string]interface{}{"scroll": e.ss.Timeout}
	sr, err := e.conn.Scroll(args, id)
	if err != nil {
		return nil, err
	}

	e.setScrollID(sr.ScrollId)
	return resultDocs(sr), nil
}

// Cleanup executes deletion of all the scroll IDs created by this runtime.
// Scrolls consume cluster memory and should always be culled after usage.
func (e Elastic5Scroll) Cleanup() ([]byte, error) {
	idlist := make([]string, 0)
	for k, _ := range e.scrollIds {
		idlist = append(idlist, k)
	}
	query := map[string]interface{}{
		"scroll_id": idlist,
	}
	fmt.Printf("scrolls: %v\n", idlist)
	url := "/_search/scroll"
	body, err := e.conn.DoCommand("DELETE", url, nil, query)
	return body, err
}

func PumpElastiScroll(ctx context.Context, es *Elastic5Scroll) (chan eskit.Doc, chan error) {
	pipe := make(chan eskit.Doc)
	errchan := make(chan error)
	doccnt := 0

	go func() {
		for {
			docs, err := es.Scroll(es.ScrollID())
			if err != nil {
				errchan <- err
			}
			for _, d := range docs {
				pipe <- *d
				doccnt++
			}
			// if docs empty close and exit
			if docs == nil {
				close(pipe)
				return
			}
		}
	}()
	return pipe, errchan
}

// ScrollID returns the current scroll idenitifier string
func (e Elastic5Scroll) ScrollID() string {
	return e.scrollId
}

func (e *Elastic5Scroll) setScrollID(id string) {
	// lock under mutex because maps and sensitive state is important.
	e.scrollmux.Lock()
	e.scrollIds[id] = struct{}{}
	e.scrollId = id
	e.scrollmux.Unlock()
}

func resultDocs(sr elastigo.SearchResult) []*eskit.Doc {
	docs := make([]*eskit.Doc, 0)
	for _, h := range sr.Hits.Hits {
		d := &eskit.Doc{
			Meta: eskit.Meta{
				ID:    h.Id,
				Type:  h.Type,
				Index: h.Index,
			},
			Source: *h.Source,
		}
		docs = append(docs, d)
	}
	if len(docs) < 1 {
		return nil
	}
	return docs
}
