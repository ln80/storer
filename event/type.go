package event

import (
	"context"
	"reflect"
	"strings"
)

// TypeOf return the type of a a value or pointer
func TypeOf(v interface{}) string {
	if v == nil {
		return ""
	}
	if v, ok := v.(interface{ Type() string }); ok {
		return v.Type()
	}

	if v, ok := v.(map[string]interface{}); ok {
		if typ, ok := v["type"].(string); ok {
			return typ
		}
	}

	rType := reflect.TypeOf(v)
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	return rType.String()
}

func TypeOfWithContext(ctx context.Context, v interface{}) string {
	t := TypeOf(v)
	if ctx.Value(ContextNamespaceKey) != nil {
		parts := strings.Split(t, ".")
		return ctx.Value(ContextNamespaceKey).(string) + "." + parts[len(parts)-1]
	}

	return t
}
