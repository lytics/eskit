package eskit

import (
	"testing"
	"time"
)

// This test uses the 'shakespeare' index example
// https://www.elastic.co/guide/en/kibana/current/tutorial-load-dataset.html
func TestIntegrationEsScroller(t *testing.T) {
	e5s := NewElastic5Scroll([]string{"127.0.0.1"}, "shakespeare", "1m", int64(10))

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"size": int64(100),
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
	docs2, err := e5s.Scroll()
	if err != nil {
		t.Errorf("error scrolling: %v", err)
	}
	if len(docs2) < 1 {
		t.Errorf("scrolled documents returned is zero!")
	}

	//t.Logf("docs2: %#v", docs2)
}
