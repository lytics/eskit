package eskit

import (
	"testing"
	"time"
)

// Integration tests against Elasticsearch 2.3.x and 5.2.x
// Tests use the 'shakespeare' index example
// https://www.elastic.co/guide/en/kibana/current/tutorial-load-dataset.html
// ...This was done for rapid prototyping and validation;
// TODO: improve reproducibility of testing for contributors

var (
	es5port = "9201"
	es2port = "9200"
	ports   = []string{es5port, es2port}
)

func TestIntegrationScrollers(t *testing.T) {
	for _, p := range ports {
		t.Logf("testing scroll for %s", p)
		testIntegrationEsScroller(t, p)
	}
}

// This test uses the 'shakespeare' index example
// https://www.elastic.co/guide/en/kibana/current/tutorial-load-dataset.html
func testIntegrationEsScroller(t *testing.T, port string) {
	ss := ScrollSettings{
		Hosts:    []string{"127.0.0.1"},
		Index:    "shakespeare",
		Port:     port,
		Timeout:  "1m",
		Pagesize: int64(10),
	}

	e5s := NewElastic5Scroll(ss)

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"size": ss.Pagesize,
		"sort": []string{"_doc"},
	}

	docs, err := e5s.Open(query)
	if err != nil {
		t.Errorf("error opening query: %v", err)
	}
	if docs == nil {
		t.Errorf("search results are nil?")
	}
	if len(docs) < 1 {
		t.Error("should be more than one doc")
	}
	t.Logf("scroll: %#v", e5s)
	time.Sleep(500 * time.Millisecond)

	// Now scroll
	docs2, err := e5s.Scroll(e5s.ScrollID())
	if err != nil {
		t.Errorf("error scrolling: %v", err)
	}
	if len(docs2) < 1 {
		t.Errorf("scrolled documents returned is zero!")
	}
	//t.Logf("docs2: %#v", docs2)
}

func TestIntegrationElasticScrollerCleanups(t *testing.T) {
	for _, p := range ports {
		t.Logf("testing port %s", p)
		testIntegrationEsScrollerCleanup(t, p)
	}
}

// This test uses the 'shakespeare' index example
// https://www.elastic.co/guide/en/kibana/current/tutorial-load-dataset.html
func testIntegrationEsScrollerCleanup(t *testing.T, port string) {
	ss := ScrollSettings{
		Hosts:    []string{"127.0.0.1"},
		Index:    "shakespeare",
		Port:     port,
		Timeout:  "1m",
		Pagesize: int64(20),
	}

	e5s := NewElastic5Scroll(ss)

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"size": ss.Pagesize,
		"sort": []string{"_doc"},
	}

	doclist := make([]*Doc, 0)

	docs, err := e5s.Open(query)
	if err != nil {
		t.Errorf("error opening query: %v", err)
	}
	if docs == nil {
		t.Errorf("search results are nil?")
	}
	if len(docs) < 1 {
		t.Error("should be more than one doc")
	}
	t.Logf("scroll: %#v", e5s)
	time.Sleep(500 * time.Millisecond)
	doclist = append(doclist, docs...)

	// Now scroll
	for i := 0; i < 10; i++ {
		docs2, err := e5s.Scroll(e5s.ScrollID())
		if err != nil {
			t.Errorf("error scrolling[%d]: %v", i, err)
		}
		if len(docs2) < 1 {
			t.Errorf("scrolled documents returned is zero![%d]", i)
		}
		doclist = append(doclist, docs2...)
	}
	if len(doclist) < 100 {
		t.Errorf("doclist should be 110 docs: %d", len(doclist))
	}

	b, err := e5s.Cleanup()
	if err != nil {
		t.Errorf("error cleaning up scrolls: %v", err)
	}
	t.Logf("%s", string(b))
}
