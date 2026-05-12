# ALOS DB Client

Go client library for ALOS DB — a high-performance document database with MongoDB-compatible queries, built-in indexing, and clustering support.

## Server Licensing

ALOS DB server is **proprietary software**. A license must be purchased to run the server in production. Contact us or visit [alos.gg](https://alos.gg) for licensing details.

## Documentation

Full documentation is available at [https://alos.gg/dbdocs/](https://alos.gg/dbdocs/).

## Installation

```bash
go get github.com/guno1928/alosdbclient
```

## Quick Start

```go
package main

import (
	"fmt"
	"log"
	"time"

	alosdbclient "github.com/guno1928/alosdbclient"
)

func main() {
	db, err := alosdbclient.Connect("localhost:6900",
		alosdbclient.WithCredentials("admin", "secret"),
		alosdbclient.WithTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	coll := db.Collection("users")

	// Insert
	id, _ := coll.InsertOne(alosdbclient.Document{
		"name":  "Alice",
		"email": "alice@example.com",
		"age":   30,
	})
	fmt.Println("Inserted:", id)

	// Find
	user, _ := coll.FindOne(alosdbclient.Document{"_id": id})
	fmt.Println("Found:", user)
}
```

## Query Protection

When using untrusted input in queries, always validate and sanitize values before constructing documents. Untrusted input containing operator keys such as `$ne` can bypass authentication checks or leak data.

```go
// Validate input before use
if username == "" || password == "" {
	log.Fatal("missing credentials")
}

user, _ := coll.FindOne(alosdbclient.Document{
	"username": username,
	"password": password,
})
```

For server-side literal-value helpers that prevent operator injection, see the [ALOS DB documentation](https://alos.gg/dbdocs/).

## Configuration Options

| Option | Description |
|--------|-------------|
| `WithBatchSize(n)` | Batch size for request pipelining |
| `WithFlushInterval(d)` | Max time to wait before flushing a batch |
| `WithTimeout(d)` | Per-request timeout |
| `WithDatabase(name)` | Target database name |
| `WithCredentials(user, pass)` | Authentication |

## Transactions

```go
err := db.Transaction(func(tx alosdbclient.TransactionInterface) error {
	users := tx.Collection("users")
	users.InsertOne(alosdbclient.Document{"name": "Bob"})
	users.UpdateOne(
		alosdbclient.Document{"name": "Alice"},
		alosdbclient.Document{"$set": alosdbclient.Document{"age": 31}},
	)
	return nil
})
```

## License

The ALOS DB server is proprietary software requiring a purchased license. This client library is provided for use with a licensed ALOS DB server.
