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
		int64, // pagesize(ignored by file reader)
	) ([]*Doc, error) // CTF: Stdin?

	Scroll(
		string, // scroll ID, filename
	) ([]*Doc, error)
}

type Elastic5Scroll struct {
	// Configuration
	index         string
	scrolltimeout string
	scrollId      string
	scrollmux     sync.Mutex
	pageSize      int64

	// Elasticsearch client? || Raw HTTP reqs?
	conn *elastigo.Conn

	// State
	scrollIds []string
}

type ElasticConnection struct {
	hosts []string
	port  string
}

func NewElastic5Scroll(ips []string, index string, scrolltimeout string, pagesize int64) *Elastic5Scroll {
	c := elastigo.NewConn()
	c.SetHosts(ips)
	c.SetPort("9201")

	return &Elastic5Scroll{
		scrolltimeout: scrolltimeout,
		pageSize:      pagesize,
		conn:          c,
	}

}

type scrollArgs struct {
	Size int64    `json:"size"`
	Sort []string `json:"sort"`
}

func (e *Elastic5Scroll) Open(query map[string]interface{}) ([]*Doc, error) {
	args := map[string]interface{}{"scroll": e.scrolltimeout}
	qb, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	sr, err := e.conn.Search(e.index, "", args, string(qb))
	if err != nil {
		return nil, err
	}
	e.setScrollID(sr.ScrollId)

	return resultDocs(sr), nil
}

func (e *Elastic5Scroll) Scroll() ([]*Doc, error) {
	args := make(map[string]interface{})
	args["scroll"] = e.scrolltimeout
	e.scrollIds = append(e.scrollIds, e.scrollId)

	sr, err := e.conn.Scroll(args, e.scrollId)
	if err != nil {
		return nil, err
	}
	e.setScrollID(sr.ScrollId)
	return resultDocs(sr), nil
}

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
