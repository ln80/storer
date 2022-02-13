package event

import (
	"errors"
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

// Register defines the register for all domain events type that are appended into the store
type Register interface {
	Set(event interface{}) Register
	Get(name string) (interface{}, error)
	Clear()
}

type register struct {
	namespace string
}

// NewRegister returns a Register instance for a given namespace,
// and will use the global namespace if namespace param is empty
func NewRegister(namespace string) Register {
	regMu.Lock()
	defer regMu.Unlock()
	if _, ok := registry[namespace]; !ok {
		registry[namespace] = make(map[string]reflect.Type)
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
	if _, ok := registry[r.namespace]; !ok {
		registry[r.namespace] = make(map[string]reflect.Type)
	}
	registry[r.namespace][name] = eType

	return r
}

func (r *register) Get(name string) (interface{}, error) {
	regMu.RLock()
	defer regMu.RUnlock()
	ctxName := ""
	if r.namespace != "" {
		splits := strings.Split(name, ".")
		ctxName = r.namespace + "." + splits[len(splits)-1]
	}

	eType, ok := registry[r.namespace][ctxName]
	if !ok {
		eType, ok = registry[""][name]
		if !ok {
			return nil, Err(ErrNotFoundInRegistry, "", eType)
		}
	}

	return reflect.New(eType).Interface(), nil
}

func (r *register) Clear() {
	regMu.RLock()
	defer regMu.RUnlock()
	registry = make(map[string]map[string]reflect.Type)
}
