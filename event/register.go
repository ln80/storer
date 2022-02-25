package event

import (
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
)

// Register defines the register for domain events
type Register interface {
	Set(event interface{}) Register
	Get(name string) (interface{}, error)
}

type register struct {
	namespace string
}

// NewRegister returns a Register instance for a given namespace,
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

func (r *register) Set(event interface{}) Register {
	rawType := reflect.TypeOf(event)
	// if event is a pointer, convert to its value
	if rawType.Kind() == reflect.Ptr {
		rawType = rawType.Elem()
	}

	eType, name := rawType, rawType.String()
	if r.namespace != "" {
		parts := strings.Split(name, ".")
		name = r.namespace + "." + parts[len(parts)-1]
	}

	regMu.Lock()
	defer regMu.Unlock()
	registry[r.namespace][name] = eType

	return r
}

func (r *register) Get(name string) (interface{}, error) {
	regMu.Lock()
	defer regMu.Unlock()

	if r.namespace != "" {
		splits := strings.Split(name, ".")
		eType, ok := registry[r.namespace][r.namespace+"."+splits[len(splits)-1]]
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
