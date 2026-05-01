package alosdbclient

import (
	"fmt"
	"time"
)

func Str(v string) string {
	return v
}

func Int(v int) int {
	return v
}

func Int64(v int64) int64 {
	return v
}

func Float(v float64) float64 {
	return v
}

func Bool(v bool) bool {
	return v
}

func Nil() interface{} {
	return nil
}

func Time(v time.Time) time.Time {
	return v
}

func Bytes(v []byte) []byte {
	return v
}

func Arr(items ...interface{}) []interface{} {
	for _, item := range items {
		if err := validateNoOperators(item); err != nil {
			panic(fmt.Sprintf("alosdbclient.Arr: %v", err))
		}
	}
	return items
}

func Map(items ...interface{}) Document {
	if len(items)%2 != 0 {
		panic("alosdbclient.Map: items must be key-value pairs")
	}
	doc := make(Document, len(items)/2)
	for i := 0; i < len(items); i += 2 {
		key, ok := items[i].(string)
		if !ok {
			panic(fmt.Sprintf("alosdbclient.Map: key at position %d is not a string", i))
		}
		if len(key) > 0 && key[0] == '$' {
			panic(fmt.Sprintf("alosdbclient.Map: operator key %q not allowed", key))
		}
		val := items[i+1]
		if err := validateNoOperators(val); err != nil {
			panic(fmt.Sprintf("alosdbclient.Map: field %q: %v", key, err))
		}
		doc[key] = val
	}
	return doc
}

func validateNoOperators(v interface{}) error {
	switch val := v.(type) {
	case Document:
		for k, sub := range val {
			if len(k) > 0 && k[0] == '$' {
				return fmt.Errorf("operator key %q not allowed", k)
			}
			if err := validateNoOperators(sub); err != nil {
				return err
			}
		}
	case map[string]interface{}:
		for k, sub := range val {
			if len(k) > 0 && k[0] == '$' {
				return fmt.Errorf("operator key %q not allowed", k)
			}
			if err := validateNoOperators(sub); err != nil {
				return err
			}
		}
	case []interface{}:
		for _, elem := range val {
			if err := validateNoOperators(elem); err != nil {
				return err
			}
		}
	}
	return nil
}
