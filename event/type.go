package event

import (
	"context"
	"reflect"
	"strings"
)

// solveType of the given value (mainly an event),
func solveType(v interface{}) (reflect.Type, string) {
	rType := reflect.TypeOf(v)
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType, rType.String()
}

// TypeOf returns the type of a value or its pointer
func TypeOf(v interface{}) (vtype string) {
	if v == nil {
		return ""
	}
	_, vtype = solveType(v)
	return
}

// TypeOfWithNamspace returns the type of the value using the given namepspace.
// by default the type name / value is {package name}.{value type name}.
// The return is changed to {namespace}.{value type name} id namespace is not empty
func TypeOfWithNamspace(namespace string, v interface{}) string {
	t := TypeOf(v)
	if namespace != "" {
		parts := strings.Split(t, ".")
		return namespace + "." + parts[len(parts)-1]
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
