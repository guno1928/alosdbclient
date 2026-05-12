package alosdbclient

import "github.com/vmihailenco/msgpack/v5"

const compressionThreshold = 1024

type opCode uint8

const (
	opInsertOne  opCode = 1
	opInsertMany opCode = 2
	opFindOne    opCode = 3
	opFind       opCode = 4
	opUpdateOne  opCode = 5
	opUpdateMany opCode = 6
	opDeleteOne  opCode = 7
	opDeleteMany opCode = 8
	opCount      opCode = 9
	opFindByID   opCode = 10
	opDrop       opCode = 12
	opStats      opCode = 13
	opFlush      opCode = 14
	opBatch      opCode = 15
	opTxBatch    opCode = 16
	opUpsertOne         opCode = 17
	opUpsertMany        opCode = 18
	opDBExists          opCode = 19
	opCollectionExists  opCode = 20
	opCreateCollection  opCode = 23
	opAggregate         opCode = 24
	opBeginTx           opCode = 25
	opTxFindOne         opCode = 26
	opTxFindByID        opCode = 27
	opTxInsertOne       opCode = 28
	opTxUpdateOne       opCode = 29
	opTxDeleteOne       opCode = 30
	opTxCommit          opCode = 31
	opTxRollback        opCode = 32
)

type batchRequest struct {
	Requests []request `msgpack:"requests"`
}

type batchResponse struct {
	IsBatch   bool       `msgpack:"is_batch"`
	Responses []response `msgpack:"responses"`
}

type request struct {
	ID         uint32             `msgpack:"id"`
	Op         opCode             `msgpack:"op"`
	Collection string             `msgpack:"collection"`
	Database   string             `msgpack:"database,omitempty"`
	Args       msgpack.RawMessage `msgpack:"args"`
	AuthToken  string             `msgpack:"auth,omitempty"`
	NoReply    bool               `msgpack:"nr,omitempty"`
}

type response struct {
	ID         uint32             `msgpack:"id"`
	Error      string             `msgpack:"error,omitempty"`
	Result     msgpack.RawMessage `msgpack:"result,omitempty"`
	Compressed bool               `msgpack:"compressed,omitempty"`
}

type compressedResponse struct {
	Data         []byte `msgpack:"data"`
	OriginalSize int    `msgpack:"orig_size"`
}

type insertOneArgs struct {
	Doc map[string]interface{} `msgpack:"doc"`
}

type insertManyArgs struct {
	IDs     []string `msgpack:"ids"`
	Blob    []byte   `msgpack:"blob"`
	Offsets []uint32 `msgpack:"offsets"`
}

type findArgs struct {
	Query map[string]interface{} `msgpack:"query"`
}

type findByIDArgs struct {
	ID string `msgpack:"id"`
}

type updateArgs struct {
	Filter map[string]interface{} `msgpack:"filter"`
	Update map[string]interface{} `msgpack:"update"`
}

type existsArgs struct {
	Name string `msgpack:"name"`
}

type existsResult struct {
	Exists bool `msgpack:"exists"`
}

type deleteArgs struct {
	Filter map[string]interface{} `msgpack:"filter"`
}

type indexArgs struct {
	Field  string `msgpack:"field"`
	Unique bool   `msgpack:"unique"`
}

type txOperation struct {
	Op         opCode                 `msgpack:"op"`
	Collection string                 `msgpack:"collection"`
	Args       map[string]interface{} `msgpack:"args"`
}

type txBatchRequest struct {
	Operations []txOperation `msgpack:"operations"`
}

type txBatchResponse struct {
	Success    bool       `msgpack:"success"`
	Error      string     `msgpack:"error,omitempty"`
	Results    []txResult `msgpack:"results,omitempty"`
	RolledBack bool       `msgpack:"rolled_back"`
}

type txResult struct {
	Success bool   `msgpack:"success"`
	Error   string `msgpack:"error,omitempty"`
	ID      string `msgpack:"id,omitempty"`
}

type aggregateArgs struct {
	Pipeline []map[string]interface{} `msgpack:"pipeline"`
}

type txIDArgs struct {
	TxID string `msgpack:"tx_id"`
}

type txIDResult struct {
	TxID string `msgpack:"tx_id"`
}

type txOpArgs struct {
	TxID       string                 `msgpack:"tx_id"`
	Collection string                 `msgpack:"collection,omitempty"`
	Filter     map[string]interface{} `msgpack:"filter,omitempty"`
	Update     map[string]interface{} `msgpack:"update,omitempty"`
	Doc        map[string]interface{} `msgpack:"doc,omitempty"`
	Query      map[string]interface{} `msgpack:"query,omitempty"`
	ID         string                 `msgpack:"id,omitempty"`
}
