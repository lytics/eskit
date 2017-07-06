package eskit

import (
	"encoding/json"
	"fmt"
	"sync"

	elastigo "github.com/mattbaird/elastigo/lib"
)

var _ Scroller = (*Elastic5Scroll)(nil)

type Scroller interface {
	// Open the Scroller to initialize any necessary state
	Open(map[string]interface{}) ([]*Doc, error) // CTF: Stdin?

	// Scroll processes the identified resource and returns documents
	Scroll(string) ([]*Doc, error)
}

type ScrollSettings struct {
	Hosts    []string
	Port     string
	Index    string
	Pagesize int64
	Timeout  string
}

type Elastic5Scroll struct {
	// Configuration
	ss ScrollSettings

	// Client
	conn *elastigo.Conn

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
func (e *Elastic5Scroll) Open(query map[string]interface{}) ([]*Doc, error) {
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
func (e *Elastic5Scroll) Scroll(id string) ([]*Doc, error) {
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

func resultDocs(sr elastigo.SearchResult) []*Doc {
	docs := make([]*Doc, 0)
	for _, h := range sr.Hits.Hits {
		d := &Doc{
			Meta: Meta{
				ID:    h.Id,
				Type:  h.Type,
				Index: h.Index,
			},
			Source: *h.Source,
		}
		docs = append(docs, d)
	}
	return docs
}
