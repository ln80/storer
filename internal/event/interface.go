package event

// Persister define the service that persists chunks in a durable store e.g S3
// type Persister interface {
// 	Persist(ctx context.Context, stmID event.StreamID, evts event.Stream) error
// }

type Transformer interface {
	Transform(fn func(curr interface{}) interface{})
}
