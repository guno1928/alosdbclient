package alosdbclient

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

const (
	defaultBatchSize            = 100
	defaultFlushInterval        = 1 * time.Millisecond
	defaultClientCacheTTL       = 135 * time.Millisecond
	defaultClientRequestTimeout = 15 * time.Second
)

// ClientConfig configures a client connection to a remote ALOS DB server.
//
// ServerAddr is the TCP address of the server (e.g. "localhost:6900").
//
// PoolSize is the number of pooled connections. Default is 10.
//
// Username is the authentication username.
//
// Password is the authentication password.
//
// RequestTimeout is the per-request timeout. Default is 15 seconds.
//
// CacheTTL is the client cache time-to-live. Default is 135 milliseconds.
//
// DisableCache disables client-side caching.
//
// FireAndForget sends requests without waiting for a response.
type ClientConfig struct {
	ServerAddr     string
	PoolSize       int
	Username       string
	Password       string
	RequestTimeout time.Duration
	CacheTTL       time.Duration
	DisableCache   bool
	FireAndForget  bool
}

// DefaultClientConfig returns a ClientConfig with sensible defaults.
func DefaultClientConfig(serverAddr string) *ClientConfig {
	return &ClientConfig{
		ServerAddr:     serverAddr,
		PoolSize:       10,
		RequestTimeout: defaultClientRequestTimeout,
		CacheTTL:       defaultClientCacheTTL,
	}
}

type pendingRequest struct {
	req      request
	respChan chan *response
}

type cacheEntry struct {
	data      []byte
	expiresAt time.Time
	gen       uint64
}

type clientCache struct {
	mu      sync.RWMutex
	entries map[string]cacheEntry
	ttl     time.Duration
	gen     uint64
	stopCh  chan struct{}
}

func newClientCache(ttl time.Duration) *clientCache {
	if ttl <= 0 {
		return nil
	}
	c := &clientCache{
		entries: make(map[string]cacheEntry),
		ttl:     ttl,
		stopCh:  make(chan struct{}),
	}
	go c.cleanupLoop()
	return c
}

func (c *clientCache) get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.RLock()
	entry, ok := c.entries[key]
	gen := c.gen
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}
	if entry.gen < gen {
		return nil, false
	}
	if time.Now().After(entry.expiresAt) {
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
		return nil, false
	}
	return entry.data, true
}

func (c *clientCache) set(key string, data []byte) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.entries[key] = cacheEntry{
		data:      data,
		expiresAt: time.Now().Add(c.ttl),
		gen:       c.gen,
	}
	c.mu.Unlock()
}

func (c *clientCache) invalidate(prefix string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	for key := range c.entries {
		if strings.HasPrefix(key, prefix) {
			delete(c.entries, key)
		}
	}
	c.mu.Unlock()
}

func (c *clientCache) invalidateAll() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.gen++
	c.mu.Unlock()
}

func (c *clientCache) cleanupLoop() {
	if c == nil {
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			c.mu.Lock()
			for key, entry := range c.entries {
				if now.After(entry.expiresAt) {
					delete(c.entries, key)
				}
			}
			c.mu.Unlock()
		case <-c.stopCh:
			return
		}
	}
}

func (c *clientCache) stop() {
	if c == nil {
		return
	}
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
}

// Client manages a connection to a remote ALOS DB server.
type Client struct {
	transport     clientTransport
	serverAddr    string
	poolSize      int
	reqID         atomic.Uint32
	timeout       time.Duration
	batchMu       sync.Mutex
	batch         []pendingRequest
	batchSize     int
	flushInterval time.Duration
	flushChan     chan bool
	stopFlush     chan bool
	flushWg       sync.WaitGroup
	cache         *clientCache
	authToken     string
	psk           []byte
	fireAndForget bool
	dbName        string
	optionErr     error
}

// ClientOption is a functional option for configuring a client connection.
type ClientOption func(*Client)

// WithBatchSize sets the batch size for client requests.
//
// Example:
//
//	db, err := alosdbclient.Connect("localhost:6900", alosdbclient.WithBatchSize(500))
func WithBatchSize(size int) ClientOption {
	return func(c *Client) {
		c.batchSize = size
	}
}

// WithFlushInterval sets the maximum time to wait before sending a batch.
//
// Example:
//
//	db, err := alosdbclient.Connect("localhost:6900", alosdbclient.WithFlushInterval(5*time.Millisecond))
func WithFlushInterval(interval time.Duration) ClientOption {
	return func(c *Client) {
		c.flushInterval = interval
	}
}

// WithTimeout sets the per-request timeout for a client connection.
//
// Example:
//
//	db, err := alosdbclient.Connect("localhost:6900", alosdbclient.WithTimeout(30*time.Second))
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		if timeout <= 0 {
			c.optionErr = fmt.Errorf("client timeout must be > 0")
			return
		}
		c.timeout = timeout
		if c.transport != nil {
			setTransportTimeout(c.transport, timeout)
		}
	}
}

// WithCacheTTL sets the cache time-to-live for the client.
//
// Example:
//
//	db, err := alosdbclient.Connect("localhost:6900", alosdbclient.WithCacheTTL(200*time.Millisecond))
func WithCacheTTL(ttl time.Duration) ClientOption {
	return func(c *Client) {
		if c.cache != nil {
			c.cache.stop()
		}
		c.cache = newClientCache(ttl)
	}
}

// WithoutCache disables client-side caching entirely.
//
// Example:
//
//	db, err := alosdbclient.Connect("localhost:6900", alosdbclient.WithoutCache())
func WithoutCache() ClientOption {
	return func(c *Client) {
		if c.cache != nil {
			c.cache.stop()
		}
		c.cache = nil
	}
}

// WithDatabase sets the target database name for all requests.
//
// Example:
//
//	db, err := alosdbclient.Connect("localhost:6900", alosdbclient.WithDatabase("analytics"))
func WithDatabase(name string) ClientOption {
	return func(c *Client) {
		c.dbName = name
	}
}

// WithCredentials sets the username and password for client authentication.
//
// Example:
//
//	db, err := alosdbclient.Connect("localhost:6900", alosdbclient.WithCredentials("admin", "secret"))
func WithCredentials(username, password string) ClientOption {
	return func(c *Client) {
		h := sha256.Sum256([]byte(username + ":" + password))
		c.authToken = hex.EncodeToString(h[:])
		c.psk = derivePSK(username, password)

		if c.transport == nil || c.serverAddr == "" {
			return
		}

		transport, err := newPooledTransport(c.serverAddr, c.poolSize, c.psk)
		if err != nil {
			c.optionErr = err
			return
		}
		setTransportTimeout(transport, c.timeout)

		oldTransport := c.transport
		c.transport = transport
		if oldTransport != nil {
			_ = oldTransport.close()
		}
	}
}

func newClientWithConfig(config *ClientConfig, opts ...ClientOption) (*Client, error) {
	poolSize := config.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	requestTimeout := config.RequestTimeout
	if requestTimeout <= 0 {
		requestTimeout = defaultClientRequestTimeout
	}

	cacheTTL := config.CacheTTL
	if cacheTTL <= 0 && !config.DisableCache {
		cacheTTL = defaultClientCacheTTL
	}

	var psk []byte
	var authToken string
	if config.Username != "" && config.Password != "" {
		h := sha256.Sum256([]byte(config.Username + ":" + config.Password))
		authToken = hex.EncodeToString(h[:])
		psk = derivePSK(config.Username, config.Password)
	}

	transport, err := newPooledTransport(config.ServerAddr, poolSize, psk)
	if err != nil {
		return nil, err
	}
	setTransportTimeout(transport, requestTimeout)

	c := &Client{
		transport:     transport,
		serverAddr:    config.ServerAddr,
		poolSize:      poolSize,
		timeout:       requestTimeout,
		batch:         make([]pendingRequest, 0, defaultBatchSize),
		batchSize:     defaultBatchSize,
		flushInterval: defaultFlushInterval,
		flushChan:     make(chan bool, 1),
		stopFlush:     make(chan bool),
		cache:         newClientCache(cacheTTL),
		authToken:     authToken,
		psk:           psk,
		fireAndForget: config.FireAndForget,
	}
	if config.DisableCache {
		c.cache = nil
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.optionErr != nil {
		if c.cache != nil {
			c.cache.stop()
		}
		if c.transport != nil {
			_ = c.transport.close()
		}
		return nil, c.optionErr
	}

	c.flushWg.Add(1)
	go c.batchFlusher()

	return c, nil
}

func newClient(serverAddr string, opts ...ClientOption) (*Client, error) {
	config := DefaultClientConfig(serverAddr)
	return newClientWithConfig(config, opts...)
}

// Close flushes pending writes and closes the connection.
func (c *Client) Close() {
	c.Flush()
	close(c.stopFlush)
	c.flushWg.Wait()

	if c.cache != nil {
		c.cache.stop()
	}

	if c.transport != nil {
		c.transport.close()
	}
}

// Flush sends any pending batched requests immediately.
func (c *Client) Flush() error {
	c.batchMu.Lock()
	defer c.batchMu.Unlock()

	if len(c.batch) == 0 {
		return nil
	}

	return c.flushBatchLocked()
}

func (c *Client) flushBatchLocked() error {
	if len(c.batch) == 0 {
		return nil
	}

	requests := make([]request, len(c.batch))
	for i, pr := range c.batch {
		requests[i] = pr.req
	}

	batchReq := batchRequest{Requests: requests}
	batchData, err := msgpack.Marshal(batchReq)
	if err != nil {
		for _, pr := range c.batch {
			pr.respChan <- &response{Error: err.Error()}
		}
		c.batch = c.batch[:0]
		return err
	}

	req := request{
		ID:        0,
		Op:        opBatch,
		Args:      batchData,
		AuthToken: c.authToken,
		Database:  c.dbName,
	}
	reqData, err := msgpack.Marshal(req)
	if err != nil {
		for _, pr := range c.batch {
			pr.respChan <- &response{Error: err.Error()}
		}
		c.batch = c.batch[:0]
		return err
	}

	respData, err := c.transport.send(reqData)
	if err != nil {
		for _, pr := range c.batch {
			pr.respChan <- &response{Error: err.Error()}
		}
		c.batch = c.batch[:0]
		return err
	}

	var resp response
	if err := msgpack.Unmarshal(respData, &resp); err != nil {
		for _, pr := range c.batch {
			pr.respChan <- &response{Error: err.Error()}
		}
		c.batch = c.batch[:0]
		return err
	}

	var batchResp batchResponse
	if err := msgpack.Unmarshal(resp.Result, &batchResp); err != nil {
		for _, pr := range c.batch {
			pr.respChan <- &response{Error: err.Error()}
		}
		c.batch = c.batch[:0]
		return err
	}

	for i, pr := range c.batch {
		if i < len(batchResp.Responses) {
			pr.respChan <- &batchResp.Responses[i]
		} else {
			pr.respChan <- &response{Error: "missing response"}
		}
	}

	c.batch = c.batch[:0]
	return nil
}

func (c *Client) batchFlusher() {
	defer c.flushWg.Done()

	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.batchMu.Lock()
			if len(c.batch) > 0 {
				c.flushBatchLocked()
			}
			c.batchMu.Unlock()
		case <-c.flushChan:
			c.batchMu.Lock()
			if len(c.batch) > 0 {
				c.flushBatchLocked()
			}
			c.batchMu.Unlock()
		case <-c.stopFlush:
			return
		}
	}
}

func (c *Client) call(op opCode, collName string, args interface{}) (*response, error) {
	enc := getEncoder()
	if err := enc.encoder.Encode(args); err != nil {
		putEncoder(enc)
		return nil, err
	}
	src := enc.buffer.Bytes()
	argsData := make([]byte, len(src))
	copy(argsData, src)
	putEncoder(enc)

	req := request{
		ID:         c.reqID.Add(1),
		Op:         op,
		Collection: collName,
		Args:       argsData,
		AuthToken:  c.authToken,
		Database:   c.dbName,
	}

	respChan := respChanPool.Get().(chan *response)

	c.batchMu.Lock()
	c.batch = append(c.batch, pendingRequest{req: req, respChan: respChan})
	shouldFlush := len(c.batch) >= c.batchSize
	c.batchMu.Unlock()

	if shouldFlush {
		select {
		case c.flushChan <- true:
		default:
		}
	}

	timer := time.NewTimer(c.timeout)
	select {
	case resp := <-respChan:
		timer.Stop()
		respChanPool.Put(respChan)
		if resp.Error != "" {
			return nil, errors.New(resp.Error)
		}
		return resp, nil
	case <-timer.C:
		go func() {
			<-respChan
			respChanPool.Put(respChan)
		}()
		return nil, errors.New("request timeout")
	}
}

func (c *Client) callDirect(op opCode, collName string, args interface{}) (*response, error) {
	argsEnc := getEncoder()
	if err := argsEnc.encoder.Encode(args); err != nil {
		putEncoder(argsEnc)
		return nil, err
	}
	argsData := argsEnc.buffer.Bytes()

	req := request{
		ID:         c.reqID.Add(1),
		Op:         op,
		Collection: collName,
		Args:       argsData,
		AuthToken:  c.authToken,
		Database:   c.dbName,
	}

	reqEnc := getEncoder()
	if err := reqEnc.encoder.Encode(req); err != nil {
		putEncoder(reqEnc)
		putEncoder(argsEnc)
		return nil, err
	}

	putEncoder(argsEnc)

	respData, err := c.transport.send(reqEnc.buffer.Bytes())
	putEncoder(reqEnc)
	if err != nil {
		return nil, err
	}

	dec := getDecoder(respData)
	var resp response
	err = dec.decode(&resp)
	putDecoder(dec)
	if err != nil {
		return nil, err
	}

	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return &resp, nil
}

type remoteCollection struct {
	client *Client
	name   string
}

func (rc *remoteCollection) cacheKey(op string, query Document) string {
	if rc.client == nil || rc.client.cache == nil {
		return ""
	}
	return rc.name + ":" + op + ":" + fmt.Sprint(query)
}

func (rc *remoteCollection) invalidateCache() {
	if rc.client != nil && rc.client.cache != nil {
		rc.client.cache.invalidate(rc.name + ":")
	}
}

// InsertOne inserts a single document. In async mode the ID is pre-generated
// client-side before sending, so the returned ID is always valid even when
// the server processes the insert in the background.
func (rc *remoteCollection) InsertOne(doc Document) (string, error) {
	if doc.GetID() == "" {
		doc["_id"] = generateDocID()
	}
	id := doc.GetID()

	// Use callDirect with raw doc. The server expects types.Document
	// (map[string]interface{}) not a wrapper struct like insertOneArgs.
	resp, err := rc.client.callDirect(opInsertOne, rc.name, doc)
	if err != nil {
		return "", err
	}
	rc.invalidateCache()

	// In async mode the server sends an empty ack; the actual result is
	// processed in the background and not returned. Return the pre-gen ID.
	if resp != nil && len(resp.Result) > 0 {
		var result struct {
			ID string `msgpack:"id"`
		}
		if err := msgpack.Unmarshal(resp.Result, &result); err == nil && result.ID != "" {
			return result.ID, nil
		}
	}
	return id, nil
}

func (rc *remoteCollection) InsertMany(docs []Document) ([]string, error) {
	ids := make([]string, len(docs))
	offsets := make([]uint32, len(docs))

	enc := getEncoder()
	for i, doc := range docs {
		offsets[i] = uint32(enc.buffer.Len())
		if err := enc.encoder.Encode(doc); err != nil {
			putEncoder(enc)
			return nil, err
		}
		if doc.GetID() != "" {
			ids[i] = doc.GetID()
		}
	}
	blob := make([]byte, enc.buffer.Len())
	copy(blob, enc.buffer.Bytes())
	putEncoder(enc)

	resp, err := rc.client.callDirect(opInsertMany, rc.name, insertManyArgs{
		IDs:     ids,
		Blob:    blob,
		Offsets: offsets,
	})
	if err != nil {
		return nil, err
	}

	rc.invalidateCache()

	var result struct {
		IDs []string `msgpack:"ids"`
	}
	if err := msgpack.Unmarshal(resp.Result, &result); err != nil {
		return nil, err
	}
	return result.IDs, nil
}

func (rc *remoteCollection) InsertManyRaw(rawDataMap map[string][]byte) error {
	return fmt.Errorf("InsertManyRaw not supported over network client")
}

func (rc *remoteCollection) FindOne(query Document) (Document, error) {
	id, isIDQuery := query["_id"].(string)
	if isIDQuery && len(query) == 1 {
		cacheKey := rc.cacheKey("fid", query)
		if cached, ok := rc.client.cache.get(cacheKey); ok {
			var doc Document
			if err := msgpack.Unmarshal(cached, &doc); err == nil {
				return doc, nil
			}
		}

		resp, err := rc.client.call(opFindByID, rc.name, findByIDArgs{ID: id})
		if err != nil {
			return nil, err
		}

		var doc Document
		if err := msgpack.Unmarshal(resp.Result, &doc); err != nil {
			return nil, err
		}
		if cacheKey != "" {
			rc.client.cache.set(cacheKey, resp.Result)
		}
		return doc, nil
	}

	cacheKey := rc.cacheKey("fo", query)
	if cached, ok := rc.client.cache.get(cacheKey); ok {
		var doc Document
		if err := msgpack.Unmarshal(cached, &doc); err == nil {
			return doc, nil
		}
	}

	resp, err := rc.client.call(opFindOne, rc.name, findArgs{Query: query})
	if err != nil {
		return nil, err
	}

	var doc Document
	if err := msgpack.Unmarshal(resp.Result, &doc); err != nil {
		return nil, err
	}
	if cacheKey != "" {
		rc.client.cache.set(cacheKey, resp.Result)
	}
	return doc, nil
}

func (rc *remoteCollection) FindOneReadonly(query Document) (Document, error) {
	return rc.FindOne(query)
}

func (rc *remoteCollection) FindMany(query Document) ([]Document, error) {
	cacheKey := rc.cacheKey("fm", query)
	if cached, ok := rc.client.cache.get(cacheKey); ok {
		var docs []Document
		if err := msgpack.Unmarshal(cached, &docs); err == nil {
			return docs, nil
		}
	}

	resp, err := rc.client.call(opFind, rc.name, findArgs{Query: query})
	if err != nil {
		return nil, err
	}

	var docs []Document
	if err := msgpack.Unmarshal(resp.Result, &docs); err != nil {
		return nil, err
	}
	if cacheKey != "" {
		rc.client.cache.set(cacheKey, resp.Result)
	}
	return docs, nil
}

func (rc *remoteCollection) FindManyReadonly(query Document) ([]Document, error) {
	return rc.FindMany(query)
}

func (rc *remoteCollection) UpdateOne(filter Document, update Document) error {
	_, err := rc.client.call(opUpdateOne, rc.name, updateArgs{Filter: filter, Update: update})
	rc.invalidateCache()
	return err
}

func (rc *remoteCollection) DeleteOne(filter Document) error {
	_, err := rc.client.call(opDeleteOne, rc.name, deleteArgs{Filter: filter})
	rc.invalidateCache()
	return err
}

// DeleteMany deletes all documents matching filter. Uses callDirect to bypass
// the async batching path so the count is accurate in sync mode. In async mode
// returns 0 since the count is not available from the empty ack.
func (rc *remoteCollection) DeleteMany(filter Document) (int, error) {
	resp, err := rc.client.callDirect(opDeleteMany, rc.name, deleteArgs{Filter: filter})
	if err != nil {
		return 0, err
	}
	rc.invalidateCache()

	if resp != nil && len(resp.Result) > 0 {
		var result struct {
			Deleted int `msgpack:"deleted"`
		}
		if err := msgpack.Unmarshal(resp.Result, &result); err == nil {
			return result.Deleted, nil
		}
	}
	return 0, nil
}

// UpdateMany updates all documents matching filter. Uses callDirect to bypass
// the async batching path so the count is accurate in sync mode. In async mode
// returns 0 since the count is not available from the empty ack.
func (rc *remoteCollection) UpdateMany(filter Document, update Document) (int, error) {
	resp, err := rc.client.callDirect(opUpdateMany, rc.name, updateArgs{Filter: filter, Update: update})
	if err != nil {
		return 0, err
	}
	rc.invalidateCache()

	if resp != nil && len(resp.Result) > 0 {
		var result struct {
			Updated int `msgpack:"updated"`
		}
		if err := msgpack.Unmarshal(resp.Result, &result); err == nil {
			return result.Updated, nil
		}
	}
	return 0, nil
}

func (rc *remoteCollection) Count() int64 {
	resp, err := rc.client.call(opCount, rc.name, struct{}{})
	if err != nil {
		return 0
	}

	var result struct {
		Count int64 `msgpack:"count"`
	}
	msgpack.Unmarshal(resp.Result, &result)
	return result.Count
}

func (rc *remoteCollection) Drop() {
	rc.client.call(opDrop, rc.name, struct{}{})
	rc.invalidateCache()
}

func (rc *remoteCollection) GetName() string {
	return rc.name
}

func (rc *remoteCollection) HasCollection() (bool, error) {
	resp, err := rc.client.callDirect(opCollectionExists, rc.name, existsArgs{Name: rc.name})
	if err != nil {
		return false, err
	}
	var result existsResult
	if err := msgpack.Unmarshal(resp.Result, &result); err != nil {
		return false, err
	}
	return result.Exists, nil
}

// UpsertOne updates one document if found, otherwise inserts a new document.
// Wires directly to the server's handleUpsertOne instead of emulating with
// FindOne + InsertOne/UpdateOne (which was broken by the InsertOne async
// batching path and required extra round-trips).
func (rc *remoteCollection) UpsertOne(filter Document, update Document) (bool, error) {
	resp, err := rc.client.callDirect(opUpsertOne, rc.name, updateArgs{Filter: filter, Update: update})
	if err != nil {
		return false, err
	}
	rc.invalidateCache()

	if resp != nil && len(resp.Result) > 0 {
		var result struct {
			Inserted bool `msgpack:"inserted"`
		}
		if err := msgpack.Unmarshal(resp.Result, &result); err == nil {
			return result.Inserted, nil
		}
	}
	return false, nil
}

// UpsertMany updates or inserts documents matching filter. Wires directly to
// the server's handleUpsertMany instead of emulating with UpdateMany + InsertOne.
func (rc *remoteCollection) UpsertMany(filter Document, update Document) (int, int, error) {
	resp, err := rc.client.callDirect(opUpsertMany, rc.name, updateArgs{Filter: filter, Update: update})
	if err != nil {
		return 0, 0, err
	}
	rc.invalidateCache()

	if resp != nil && len(resp.Result) > 0 {
		var result struct {
			Matched  int `msgpack:"matched"`
			Inserted int `msgpack:"inserted"`
		}
		if err := msgpack.Unmarshal(resp.Result, &result); err == nil {
			return result.Matched, result.Inserted, nil
		}
	}
	return 0, 0, nil
}

func (rc *remoteCollection) Aggregate(pipeline []Document) ([]Document, error) {
	return nil, fmt.Errorf("aggregate not supported over network")
}

type remoteDatabase struct {
	client *Client
	name   string
}

func (db *remoteDatabase) Collection(name string) CollectionInterface {
	return &remoteCollection{
		client: db.client,
		name:   name,
	}
}

func (db *remoteDatabase) ListCollections() []string {
	return nil
}

func (db *remoteDatabase) DBExists(name string) (bool, error) {
	resp, err := db.client.callDirect(opDBExists, "", existsArgs{Name: name})
	if err != nil {
		return false, err
	}
	var result existsResult
	if err := msgpack.Unmarshal(resp.Result, &result); err != nil {
		return false, err
	}
	return result.Exists, nil
}

func (db *remoteDatabase) GetStats() map[string]interface{} {
	resp, err := db.client.call(opStats, "", struct{}{})
	if err != nil {
		return nil
	}

	var result map[string]interface{}
	msgpack.Unmarshal(resp.Result, &result)
	return result
}

func (db *remoteDatabase) Close() error {
	db.client.Close()
	return nil
}

func (db *remoteDatabase) BeginTransaction() TransactionInterface {
	return &remoteTransaction{
		db:         db,
		operations: make([]txOperation, 0),
		state:      0,
	}
}

func (db *remoteDatabase) Transaction(fn func(tx TransactionInterface) error) error {
	remoteTx := db.BeginTransaction()
	defer remoteTx.Rollback()

	if err := fn(remoteTx); err != nil {
		return err
	}

	return remoteTx.Commit()
}

func (db *remoteDatabase) Export(w io.Writer, collections []string) error {
	return fmt.Errorf("Export is not supported on remote connections; run on the server")
}

func (db *remoteDatabase) Import(r io.Reader) (*ImportResult, error) {
	return nil, fmt.Errorf("Import is not supported on remote connections; run on the server")
}

// Connect connects to a remote database server using TCP.
//
// Example:
//
//	db, err := alosdbclient.Connect("localhost:6900",
//		alosdbclient.WithCredentials("admin", "secret"),
//		alosdbclient.WithTimeout(10*time.Second),
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
func Connect(serverAddr string, opts ...ClientOption) (DatabaseInterface, error) {
	client, err := newClient(serverAddr, opts...)
	if err != nil {
		return nil, err
	}
	return &remoteDatabase{
		client: client,
		name:   "default",
	}, nil
}

// ConnectWithConfig connects to a remote server using a full ClientConfig.
//
// Example:
//
//	config := &alosdbclient.ClientConfig{
//		ServerAddr: "localhost:6900",
//		PoolSize:   4,
//		Username:   "admin",
//		Password:   "secret",
//	}
//	db, err := alosdbclient.ConnectWithConfig(config)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
func ConnectWithConfig(config *ClientConfig, opts ...ClientOption) (DatabaseInterface, error) {
	client, err := newClientWithConfig(config, opts...)
	if err != nil {
		return nil, err
	}
	return &remoteDatabase{
		client: client,
		name:   "default",
	}, nil
}

type remoteTransaction struct {
	db         *remoteDatabase
	operations []txOperation
	state      int
	mu         sync.Mutex
	id         string
}

type remoteTxCollection struct {
	tx   *remoteTransaction
	name string
}

func (tx *remoteTransaction) Collection(name string) TxCollectionInterface {
	return &remoteTxCollection{
		tx:   tx,
		name: name,
	}
}

func (tc *remoteTxCollection) FindOne(query Document) (Document, error) {
	return tc.tx.db.Collection(tc.name).FindOne(query)
}

func (tc *remoteTxCollection) InsertOne(doc Document) (string, error) {
	tc.tx.mu.Lock()
	defer tc.tx.mu.Unlock()

	if tc.tx.state != 0 {
		return "", fmt.Errorf("transaction not active")
	}

	if doc.GetID() == "" {
		doc["_id"] = generateDocID()
	}
	id := doc.GetID()

	tc.tx.operations = append(tc.tx.operations, txOperation{
		Op:         opInsertOne,
		Collection: tc.name,
		Args: map[string]interface{}{
			"doc": map[string]interface{}(doc),
		},
	})

	return id, nil
}

func (tc *remoteTxCollection) UpdateOne(filter Document, update Document) error {
	tc.tx.mu.Lock()
	defer tc.tx.mu.Unlock()

	if tc.tx.state != 0 {
		return fmt.Errorf("transaction not active")
	}

	tc.tx.operations = append(tc.tx.operations, txOperation{
		Op:         opUpdateOne,
		Collection: tc.name,
		Args: map[string]interface{}{
			"filter": map[string]interface{}(filter),
			"update": map[string]interface{}(update),
		},
	})

	return nil
}

func (tc *remoteTxCollection) DeleteOne(filter Document) error {
	tc.tx.mu.Lock()
	defer tc.tx.mu.Unlock()

	if tc.tx.state != 0 {
		return fmt.Errorf("transaction not active")
	}

	tc.tx.operations = append(tc.tx.operations, txOperation{
		Op:         opDeleteOne,
		Collection: tc.name,
		Args: map[string]interface{}{
			"filter": map[string]interface{}(filter),
		},
	})

	return nil
}

func (tx *remoteTransaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != 0 {
		return fmt.Errorf("transaction not active")
	}

	if len(tx.operations) == 0 {
		tx.state = 1
		return nil
	}

	req := txBatchRequest{
		Operations: tx.operations,
	}

	resp, err := tx.db.client.call(opTxBatch, "", req)
	if err != nil {
		tx.state = 2
		return fmt.Errorf("transaction failed: %w", err)
	}

	var txResp txBatchResponse
	if err := msgpack.Unmarshal(resp.Result, &txResp); err != nil {
		tx.state = 2
		return fmt.Errorf("failed to parse transaction response: %w", err)
	}

	if !txResp.Success {
		tx.state = 2
		if txResp.RolledBack {
			return fmt.Errorf("transaction rolled back: %s", txResp.Error)
		}
		return fmt.Errorf("transaction failed: %s", txResp.Error)
	}

	tx.state = 1
	return nil
}

func (tx *remoteTransaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != 0 {
		return nil
	}

	tx.state = 2
	tx.operations = nil
	return nil
}

func (tx *remoteTransaction) GetID() string {
	if tx.id == "" {
		tx.id = generateDocID()
	}
	return tx.id
}

