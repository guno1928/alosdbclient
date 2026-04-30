package alosdbclient

import "io"

// Document is a map with string keys and arbitrary values.
type Document map[string]interface{}

// GetID returns the document's _id field as a string, or "" if not set.
func (d Document) GetID() string {
	if id, ok := d["_id"].(string); ok {
		return id
	}
	return ""
}

// CollectionInterface defines the operations available on a document collection.
type CollectionInterface interface {
	InsertOne(doc Document) (string, error)
	InsertMany(docs []Document) ([]string, error)
	InsertManyRaw(rawDataMap map[string][]byte) error
	FindOne(query Document) (Document, error)
	FindOneReadonly(query Document) (Document, error)
	FindMany(query Document) ([]Document, error)
	FindManyReadonly(query Document) ([]Document, error)
	UpdateOne(filter Document, update Document) error
	DeleteOne(filter Document) error
	DeleteMany(filter Document) (int, error)
	UpdateMany(filter Document, update Document) (int, error)
	UpsertOne(filter Document, update Document) (bool, error)
	UpsertMany(filter Document, update Document) (int, int, error)
	Aggregate(pipeline []Document) ([]Document, error)
	Count() int64
	Drop()
	GetName() string
}

// DatabaseInterface defines the operations available on a database instance.
type DatabaseInterface interface {
	Collection(name string) CollectionInterface
	ListCollections() []string
	GetStats() map[string]interface{}
	Close() error
	BeginTransaction() TransactionInterface
	Transaction(fn func(tx TransactionInterface) error) error
	Export(w io.Writer, collections []string) error
	Import(r io.Reader) (*ImportResult, error)
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
