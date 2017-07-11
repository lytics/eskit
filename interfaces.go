package eskit

import "context"

// DocScroller wraps opening and scrolling through data sets
type DocScroller interface {
	// Open the Scroller to initialize any necessary state
	Open(map[string]interface{}) ([]*Doc, error) // CTF: Stdin?

	// Scroll processes the identified resource and returns documents
	Scroll(string) ([]*Doc, error)
}

// DocScrollerContext wraps opening and scrolling through data sets
// Allows the caller to terminate operations
type DocScrollerContext interface {
	// Open the Scroller to initialize any necessary state
	Open(context.Context, map[string]interface{}) ([]*Doc, error) // CTF: Stdin?

	// Scroll processes the identified resource and returns documents
	Scroll(context.Context, string) ([]*Doc, error)
}

type DocWriter interface {
	Write(context.Context, chan Doc) chan error
}
