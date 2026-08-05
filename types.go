package alosdbclient

import "io"

// Document is a map with string keys and arbitrary values.
type Document map[string]interface{}

type IndexValueType string

const (
	IndexValueString  IndexValueType = "string"
	IndexValueInteger IndexValueType = "integer"
	IndexValueFloat   IndexValueType = "float"
	IndexValueBoolean IndexValueType = "boolean"
	IndexValueTime    IndexValueType = "time"
	IndexValueBytes   IndexValueType = "bytes"
)

type IndexField struct {
	Name      string         `json:"name" msgpack:"name"`
	ValueType IndexValueType `json:"value_type" msgpack:"value_type"`
}

type IndexTypeIssue struct {
	Position int            `json:"position" msgpack:"position"`
	ID       string         `json:"id,omitempty" msgpack:"id,omitempty"`
	Field    string         `json:"field" msgpack:"field"`
	Expected IndexValueType `json:"expected" msgpack:"expected"`
	Actual   string         `json:"actual" msgpack:"actual"`
}

type IndexBuildResult struct {
	Indexed   int64            `json:"indexed" msgpack:"indexed"`
	Skipped   int64            `json:"skipped" msgpack:"skipped"`
	Issues    []IndexTypeIssue `json:"issues,omitempty" msgpack:"issues,omitempty"`
	Truncated bool             `json:"truncated,omitempty" msgpack:"truncated,omitempty"`
}

// GetID returns the document's _id field as a string, or "" if not set.
func (d Document) GetID() string {
	if id, ok := d["_id"].(string); ok {
		return id
	}
	return ""
}

// CollectionInterface defines the operations available on a document collection.
type StreamOptions struct {
	Fields    []string
	BatchSize int
}

type CollectionInterface interface {
	InsertOne(doc Document) (string, error)
	InsertMany(docs []Document) ([]string, error)
	InsertManyRaw(rawDataMap map[string][]byte) error
	FindOne(query Document) (Document, error)
	FindOneReadonly(query Document) (Document, error)
	FindMany(query Document) ([]Document, error)
	FindManyReadonly(query Document) ([]Document, error)
	FindManyProjected(query Document, fields []string) ([]Document, error)
	FindPaginated(query Document, skip, limit int) ([]Document, error)
	FindPaginatedProjected(query Document, fields []string, skip, limit int) ([]Document, error)
	FindManyStream(query Document, opts StreamOptions, fn func([]Document) error) error
	FindManyCount(query Document) (int, error)
	UpdateOne(filter Document, update Document) error
	DeleteOne(filter Document) error
	DeleteMany(filter Document) (int, error)
	UpdateMany(filter Document, update Document) (int, error)
	UpsertOne(filter Document, update Document) (bool, error)
	UpsertMany(filter Document, update Document) (int, int, error)
	Aggregate(pipeline []Document) ([]Document, error)
	AggregateStream(pipeline []Document, opts StreamOptions, fn func([]Document) error) error
	Count() int64
	Drop()
	GetName() string
	HasCollection() (bool, error)
	CreateIndex(field string, valueType IndexValueType) (IndexBuildResult, error)
	CreateCompoundIndex(fields []IndexField) (IndexBuildResult, error)
	DropIndex(field string)
	ListIndexes() []map[string]interface{}
	RebuildIndex(field string) error
}

// DatabaseInterface defines the operations available on a database instance.
type DatabaseInterface interface {
	Collection(name string) CollectionInterface
	CreateCollection(name string) error
	ListCollections() []string
	GetStats() map[string]interface{}
	Close() error
	BeginTransaction() TransactionInterface
	Transaction(fn func(tx TransactionInterface) error) error
	Export(w io.Writer, collections []string) error
	Import(r io.Reader) (*ImportResult, error)
	DBExists(name string) (bool, error)
}

// TransactionInterface defines operations within an ACID transaction.
type TransactionInterface interface {
	Collection(name string) TxCollectionInterface
	Commit() error
	Rollback() error
	GetID() string
}

// TxCollectionInterface defines operations on a collection within a transaction.
type TxCollectionInterface interface {
	FindOne(query Document) (Document, error)
	InsertOne(doc Document) (string, error)
	UpdateOne(filter Document, update Document) error
	DeleteOne(filter Document) error
}

// ImportResult holds the result of an import operation.
type ImportResult struct {
	Total  int64
	Errors int64
}
