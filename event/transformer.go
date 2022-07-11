package event

import (
	"context"
	"fmt"
	"reflect"

	intevent "github.com/ln80/storer/internal/event"
)

// copyEvent copies evetn data to preserve original copy.
// It returns a pointer to a copy of the origin value.
// It fails if can't get the pointer address from the value copy.
func copyEvent(env Envelope) (interface{}, error) {
	if env == nil || env.Event() == nil {
		return nil, nil
	}

	evt := env.Event()
	var (
		origin, copy, rv reflect.Value
		eType            reflect.Type
	)

	rv = reflect.ValueOf(evt)

	if reflect.TypeOf(evt).Kind() == reflect.Ptr {
		// log.Println("is ptr")

		origin = rv.Elem()
		eType = rv.Elem().Type()
	} else {
		// log.Println("is not ptr")

		origin = rv
		eType = rv.Type()
	}

	copy = reflect.New(eType).Elem()
	for i := 0; i < origin.NumField(); i++ {
		if !copy.Field(i).CanSet() {
			return nil, fmt.Errorf("can't copy event field %s.%s", eType, copy.Field(i).Type().Name())
		}
		copy.Field(i).Set(origin.Field(i))
	}

	// log.Println("debug reflect val", copy, copy.Type(), copy.Kind(), copy.CanAddr())
	if !copy.CanAddr() {
		return nil, fmt.Errorf("can't obtain addr of the copy(s) %v", copy.Type())
	}

	return copy.Addr().Interface(), nil
}

// Transform the given slice of events by replacing the data (aka domain event)
// with the result of fn function.
// It copy first values and perform transformation on the copies.
// It preserve events values of all events if any error has occured during a mutation.
func Transform(ctx context.Context, envs []Envelope, fn func(ctx context.Context, copyPtrs ...interface{}) error) error {
	index := make(map[int]interface{})
	copyPtrs := []interface{}{}
	for i, env := range envs {
		if env == nil {
			continue
		}
		// ignore envelopes that don't satisfy Transformer interface
		if _, ok := env.(intevent.Transformer); !ok {
			continue
		}

		evt := env.Event()
		if evt == nil {
			continue
		}

		copy, err := copyEvent(env)
		if err != nil {
			return err
		}

		index[i] = copy
		copyPtrs = append(copyPtrs, index[i])
	}

	// fn may not be atomic. Hence copy events first and set them back after mutation
	if err := fn(ctx, copyPtrs...); err != nil {
		return err
	}

	for idx, d := range index {
		d := d
		envs[idx].(intevent.Transformer).Transform(func(curr interface{}) interface{} {
			return d
		})
	}

	return nil
}
