package event

import (
	"context"
	"reflect"
	"strings"
)

// resolveType of the given value which is mainly an event.
// It accepts both pointers and values. Pointers type are replaced with their values type.
func resolveType(v interface{}) (reflect.Type, string) {
	rType := reflect.TypeOf(v)
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType, rType.String()
}

// TypeOf returns the type of a value or its pointer
func TypeOf(v interface{}) (vtype string) {
	_, vtype = resolveType(v)
	return
}

// TypeOfWithNamspace returns the type of the value using the given namepspace.
// By default the type format is {package name}.{value type name}.
// The return is changed to {namespace}.{value type name} if namespace is not empty
func TypeOfWithNamspace(namespace string, v interface{}) string {
	t := TypeOf(v)
	if namespace != "" {
		splits := strings.Split(t, ".")
		return namespace + "." + splits[len(splits)-1]
	}
	return t
}

// TypeOfWithContext uses TypeOfWithNamspace under the hood and looks for the namespace value from the context.
func TypeOfWithContext(ctx context.Context, v interface{}) string {
	if ctx.Value(ContextNamespaceKey) != nil {
		return TypeOfWithNamspace(ctx.Value(ContextNamespaceKey).(string), v)
	}
	return TypeOf(v)
}
