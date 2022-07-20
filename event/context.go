package event

type ContextKey string

func (c ContextKey) String() string {
	return string(c)
}

var (
	ContextNamespaceKey = ContextKey("namespace")
	// ContextNamespaceKey = "namespace"
	ContextUserKey = ContextKey("user")
)
