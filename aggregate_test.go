package alosdbclient

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestAggregateArgsEncoding(t *testing.T) {
	args := aggregateArgs{
		Pipeline: []map[string]interface{}{
			{"$match": map[string]interface{}{"username": "xertz", "openstatus": "open"}},
			{"$sort": map[string]interface{}{"created_at": int64(-1)}},
			{"$skip": int64(0)},
			{"$limit": int64(10)},
		},
	}

	data, err := msgpack.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}

	var decoded aggregateArgs
	if err := msgpack.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	if len(decoded.Pipeline) != 4 {
		t.Fatalf("expected 4 stages, got %d", len(decoded.Pipeline))
	}

	matchStage := decoded.Pipeline[0]
	if _, ok := matchStage["$match"]; !ok {
		t.Fatalf("expected $match key, got %v", matchStage)
	}
}
