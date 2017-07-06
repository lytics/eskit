package eskit

import (
	"encoding/json"
	"sync"

	elastigo "github.com/mattbaird/elastigo/lib"
)

//var _ Scroller = (*Elastic5Scroll)(nil)

type Scroller interface {
	// Open the scroll or configure list of files to read
	Open(
		map[string]interface{}, // Data mapping[query, filenames]
	) ([]*Doc, error) // CTF: Stdin?

	Scroll(
		string, // scroll ID, filename
	) ([]*Doc, error)
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
	scrollIds []string
}

//func NewElastic5Scroll(ips []string, index string, scrolltimeout string, pagesize int64) *Elastic5Scroll {
func NewElastic5Scroll(ss ScrollSettings) *Elastic5Scroll {
	c := elastigo.NewConn()
	c.SetHosts(ss.Hosts)
	c.SetPort(ss.Port)

	return &Elastic5Scroll{
		ss:   ss,
		conn: c,
	}
}

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

func (e *Elastic5Scroll) Scroll() ([]*Doc, error) {
	args := map[string]interface{}{"scroll": e.ss.Timeout}
	e.scrollIds = append(e.scrollIds, e.scrollId)

	sr, err := e.conn.Scroll(args, e.scrollId)
	if err != nil {
		return nil, err
	}

	e.setScrollID(sr.ScrollId)
	return resultDocs(sr), nil
}

// Cleanup executes deletion of all the scroll IDs created by this runtime.
// Scrolls consume cluster memory and should always be culled after usage.
func (e Elastic5Scroll) Cleanup() ([]byte, error) {
	query := map[string]interface{}{
		"scroll_id": e.scrollIds,
	}
	url := "/_search/scroll"
	body, err := e.conn.DoCommand("DELETE", url, nil, query)
	return body, err
}

// ScrollID returns the current scroll idenitifier string
func (e Elastic5Scroll) ScrollID() string {
	return e.scrollId
}

func (e *Elastic5Scroll) setScrollID(id string) {
	e.scrollmux.Lock()
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
