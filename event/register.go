package event

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

var (
	regMu    sync.RWMutex
	registry = make(map[string]map[string]reflect.Type)
)

var (
	ErrNotFoundInRegistry = errors.New("event not found in registry")
	ErrConvertEventFailed = errors.New("convert event failed")
)

// Register defines the registry service for domain events
type Register interface {
	// Set register the given event in the registry.
	Set(event interface{}) Register
	// Get return an empty instance of the given event type.
	// Note that it returns the value type, not the pointer
	Get(name string) (interface{}, error)
	// Convert the given event to its equivalent from the global namespace
	Convert(evt interface{}) (interface{}, error)
	// clear all namespace registries. Its mainly used in internal tests
	clear()
}

// register implement the Register interface
// it allows to have a registry per namespace, and use the global registery (i.e, empty namespace)
// to handle some fallback logics
type register struct {
	namespace string
}

// NewRegisterFrom context returns a new instance of the register using the namespace found in the context.
// Otherwise, it returns an instance base on the global namespace
func NewRegisterFrom(ctx context.Context) Register {
	if namespace := ctx.Value(ContextNamespaceKey); namespace != nil {
		return NewRegister(namespace.(string))
	}
	return NewRegister("")
}

// NewRegister returns a Register instance for the given namespace.
func NewRegister(namespace string) Register {
	regMu.Lock()
	defer regMu.Unlock()
	if _, ok := registry[namespace]; !ok {
		registry[namespace] = make(map[string]reflect.Type)
	}
	if _, ok := registry[""]; !ok {
		registry[""] = make(map[string]reflect.Type)
	}

	return &register{namespace: namespace}
}

// Set implements Set method of the Register interface.
// It registers the given event in the current namespace registry.
// It uses TypeOfWithNamspace func to solve the event name (aka ID).
// By default the event name is {package name}.{event struct name}
// In case of namespace exists, the event name becomes {namespace}.{evnet struct name}
func (r *register) Set(evt interface{}) Register {
	name := TypeOfWithNamspace(r.namespace, evt)
	rType, _ := resolveType(evt)

	regMu.Lock()
	defer regMu.Unlock()
	registry[r.namespace][name] = rType

	return r
}

// Get implements Get method of the Register interface.
// It looks for the event in the namespace registry,
// and use the global namespace's one as fallback
func (r *register) Get(name string) (interface{}, error) {
	regMu.Lock()
	defer regMu.Unlock()

	if r.namespace != "" {
		parts := strings.Split(name, ".")
		eType, ok := registry[r.namespace][r.namespace+"."+parts[len(parts)-1]]
		if ok {
			return reflect.New(eType).Interface(), nil
		}
	}

	eType, ok := registry[r.namespace][name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFoundInRegistry, "event type: "+name)
	}

	return reflect.New(eType).Interface(), nil
}

// Convert implements Convert method of the Register interface
func (r *register) Convert(evt interface{}) (convevt interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrConvertEventFailed, r)
		}
	}()

	name := TypeOfWithNamspace(r.namespace, evt)

	regMu.Lock()
	defer regMu.Unlock()

	eType, ok := registry[""][name]
	if !ok {
		err = fmt.Errorf("%w: %s", ErrNotFoundInRegistry, "during conversion to the equivalent event from global namespace")
		return
	}
	convevt = reflect.ValueOf(evt).Convert(eType).Interface()
	return
}

//  clear implements clear method of the Register interface
func (r *register) clear() {
	regMu.Lock()
	defer regMu.Unlock()

	registry = make(map[string]map[string]reflect.Type)
}
