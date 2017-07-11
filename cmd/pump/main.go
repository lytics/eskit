package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	es "github.com/lytics/eskit/pkg/api/elasticsearch"
	"github.com/lytics/eskit/pkg/io/streams"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	doccnt := 0
	log.SetOutput(os.Stderr)

	settings := es.ScrollSettings{
		Hosts:    []string{"127.0.0.1"},
		Index:    "shakespeare",
		Port:     "9201",
		Timeout:  "1m",
		Pagesize: int64(1000),
	}
	escroll := es.NewElastic5Scroll(settings)

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"size": settings.Pagesize,
		"sort": []string{"_doc"},
	}
	docs, err := escroll.Open(query)
	if err != nil {
		log.Fatalf("error opening scroll: %v", err)
	}

	// Start scroller
	pipe, errchan := es.PumpElastiScroll(ctx, escroll)

	stdio := &streams.StdIO{}
	errsout := stdio.Write(ctx, pipe)

	go func() {
		errs := 0
		for {
			select {
			case err := <-errsout:
				log.WithError(err).Warnf("error returned from stdout writer: [%d]", errs)
			case err := <-errchan:
				log.WithError(err).Warnf("error returned from scroll pump: [%d]", errs)
			}
			errs++
			if errs > 10 {
				log.Fatal("max number of errors exceeded, exiting")
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	// Read docs
	go func() {
		log.Debug("pump reader started")
		for {
			select {
			case doc := <-pipe:
				//log.WithFields(log.Fields{"index": doc.Index, "type": doc.Type, "len": len(doc.Source)}).Info("doc recorded")
				if len(doc.Source) < 1 {
					wg.Done()
					return
				}
				doccnt++
				fmt.Printf("%s\n", string(doc.Source))
			}
		}
		log.Debug("pump pipe exited")
	}()

	for _, d := range docs {
		pipe <- *d
	}

	wg.Wait()
	select {
	case <-time.Tick(90 * time.Second):
	case <-pipe:
	}

	time.Sleep(2 * time.Second)
	cancel()
	resp, err := escroll.Cleanup()
	if err != nil {
		log.WithError(err).Warnf("cleanup had error: %s", string(resp))
	}
	log.Infof("done! %d", doccnt)
}
