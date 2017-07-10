package eskit

// DocScroller wraps opening and scrolling through data sets
type DocScroller interface {
	// Open the Scroller to initialize any necessary state
	Open(map[string]interface{}) ([]*Doc, error) // CTF: Stdin?

	// Scroll processes the identified resource and returns documents
	Scroll(string) ([]*Doc, error)
}
