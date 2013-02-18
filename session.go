package rethinkgo

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"errors"
	"fmt"
	p "github.com/christopherhesse/rethinkgo/query_language"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
)

var debugMode bool = false
var maxIdleConnections int = 2

const clientHello uint32 = 0xaf61ba35

// Session represents a connection to a server, use it to run queries against a
// database, with either sess.Run(query) or query.Run() (uses the most
// recently-created session).
type Session struct {
	// current query identifier, just needs to be unique for each query, so we
	// can match queries with responses, e.g. 4782371
	token int64
	// address of server, e.g. "localhost:28015"
	address string
	// database to use if no database is specified in query, e.g. "test"
	database string

	mutex     sync.Mutex // protects idleConns and closed
	idleConns []net.Conn
	closed    bool
}

// Query is the interface for queries that can be .Run(), this includes
// Expression (run as a read query), MetaQuery, and WriteQuery. Methods that
// generate a query are generally located on Expression objects.
type Query interface {
	toProtobuf(context) *p.Query // will panic on errors
	Run(*Session) *Rows          // TODO: should this be in the interface?
}

// SetDebug causes all queries sent to the server and responses received to be
// printed to stdout in raw form.
//
// Example usage:
//
//  r.SetDebug(true)
func SetDebug(debug bool) {
	debugMode = debug
}

// writeMessage writes a byte array to the stream preceeded by the length in
// bytes.
func writeMessage(conn net.Conn, data []byte) error {
	messageLength := uint32(len(data))
	if err := binary.Write(conn, binary.LittleEndian, messageLength); err != nil {
		return err
	}

	_, err := conn.Write(data)
	return err
}

// writeQuery writes a protobuf message to the connection.
func writeQuery(conn net.Conn, protobuf *p.Query) error {
	data, err := proto.Marshal(protobuf)
	if err != nil {
		return fmt.Errorf("rethinkdb: Could not marshal protocol buffer: %v, %v", protobuf, err)
	}

	return writeMessage(conn, data)
}

// readMessage reads a single message from a connection.  A message is a length
// followed by a serialized protocol buffer.
func readMessage(conn net.Conn) ([]byte, error) {
	var messageLength uint32
	if err := binary.Read(conn, binary.LittleEndian, &messageLength); err != nil {
		return nil, err
	}

	var result []byte
	buf := make([]byte, messageLength)
	for {
		n, err := conn.Read(buf[0:])
		if err != nil {
			return nil, err
		}
		result = append(result, buf[0:n]...)
		if len(result) == int(messageLength) {
			break
		}
	}
	return result, nil
}

// readResponse reads a protobuf message from a connection and parses it.
func readResponse(conn net.Conn) (*p.Response, error) {
	data, err := readMessage(conn)
	if err != nil {
		return nil, err
	}
	response := &p.Response{}
	err = proto.Unmarshal(data, response)
	return response, err
}

// Connect creates a new database session.
//
// Example usage:
//
//  sess, err := r.Connect("localhost:28015", "test")
func Connect(address, database string) (*Session, error) {
	s := &Session{address: address, database: database, closed: false}

	// create a connection to make sure the server works, then immediately put it
	// in the idle connection pool
	conn, err := s.getConn()
	if err != nil {
		return nil, err
	}

	s.putConn(conn)

	return s, nil
}

// Close closes the session, freeing any associated resources.
//
// Example usage:
//
//  err := sess.Close()
func (s *Session) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil
	}

	var lastError error
	for _, conn := range s.idleConns {
		err := conn.Close()
		if err != nil {
			lastError = err
		}
	}
	s.idleConns = nil
	s.closed = true

	return lastError
}

// Reconnect closes and re-opens a session.
//
// Example usage:
//
//  err := sess.Reconnect()
func (s *Session) Reconnect() error {
	if err := s.Close(); err != nil {
		return err
	}
	s.mutex.Lock()
	s.closed = false
	s.mutex.Unlock()
	return nil
}

// return a connection from the free connections list if available, otherwise,
// create a new connection
func (s *Session) getConn() (net.Conn, error) {
	s.mutex.Lock()
	if s.closed {
		s.mutex.Unlock()
		return nil, errors.New("rethinkdb: session is closed")
	}
	if len(s.idleConns) > 0 {
		conn := s.idleConns[0]
		s.idleConns = s.idleConns[1:]
		s.mutex.Unlock()
		return conn, nil
	}
	s.mutex.Unlock()

	conn, err := net.Dial("tcp", s.address)
	if err != nil {
		return nil, err
	}

	if err := binary.Write(conn, binary.LittleEndian, clientHello); err != nil {
		return nil, err
	}

	return conn, nil
}

// return a connection to the free list, or close it if we already have enough
func (s *Session) putConn(conn net.Conn) {
	s.mutex.Lock()
	if len(s.idleConns) < maxIdleConnections {
		s.idleConns = append(s.idleConns, conn)
		s.mutex.Unlock()
		return
	}
	s.mutex.Unlock()

	conn.Close()
}

// Use changes the default database for a connection.  This is the database that
// will be used when a query is created without an explicit database.  This
// should not be used if the session is shared between goroutines, confusion
// would result.
//
// Example usage:
//
//  sess.Use("dave")
//  rows := r.Table("employees").Run() // uses database "dave"
func (s *Session) Use(database string) {
	s.database = database
}

// getToken generates the next query token, used to number requests and match
// responses with requests.
func (s *Session) getToken() int64 {
	return atomic.AddInt64(&s.token, 1)
}

// executeQuery sends a single query to the server and retrieves the parsed
// response.
func executeQuery(conn net.Conn, protobuf *p.Query) (responseProto *p.Response, err error) {
	if err = writeQuery(conn, protobuf); err != nil {
		return
	}

	for {
		responseProto, err = readResponse(conn)
		if err != nil {
			return
		}

		if responseProto.GetToken() == protobuf.GetToken() {
			break
		} else if responseProto.GetToken() > protobuf.GetToken() {
			return nil, errors.New("rethinkdb: The server returned a response for a protobuf that was not submitted by us")
		}
	}
	return
}

// Run executes a query directly on a specific session and returns an iterator
// that moves through the resulting JSON rows with rows.Next(&dest). See the
// documentation for the Rows object for other options.
//
// Example usage:
//
//  rows := session.Run(query)
//  var row map[string]interface{}
//  for rows.Next(&row) {
//      fmt.Println("row:", row)
//  }
//  if rows.Err() {
//      ...
//  }
func (s *Session) Run(query Query) *Rows {
	ctx := context{databaseName: s.database}
	queryProto, err := buildProtobuf(ctx, query)
	if err != nil {
		return &Rows{lasterr: err}
	}

	queryProto.Token = proto.Int64(s.getToken())

	conn, err := s.getConn()
	if err != nil {
		return &Rows{lasterr: err}
	}

	buffer, status, err := s.run(conn, queryProto, query)
	if err != nil {
		s.putConn(conn)
		return &Rows{lasterr: err}
	}

	if status != p.Response_SUCCESS_PARTIAL {
		// if we have a success stream response, the connection needs to be tied to
		// the iterator, since the iterator can only get more results from the same
		// connection it was originally started on
		s.putConn(conn)
	}

	switch status {
	case p.Response_SUCCESS_JSON:
		// single document (or json) response, return an iterator anyway for
		// consistency of types
		return &Rows{
			buffer:   buffer,
			complete: true,
			status:   status,
		}
	case p.Response_SUCCESS_PARTIAL:
		// beginning of stream of rows, there are more results available from the
		// server than the ones we just received, so save the connection we used in
		// case the user wants more
		return &Rows{
			session:  s,
			conn:     &conn,
			buffer:   buffer,
			complete: false,
			token:    queryProto.GetToken(),
			query:    query,
			status:   status,
		}
	case p.Response_SUCCESS_STREAM:
		// end of a stream of rows, since we got this on the initial query this means
		// that we got a stream response, but the number of results was less than the
		// number required to break the response into chunks. we can just return all
		// the results in one go, as this is the only response
		return &Rows{
			buffer:   buffer,
			complete: true,
			status:   status,
		}
	case p.Response_SUCCESS_EMPTY:
		return &Rows{
			lasterr:  io.EOF,
			complete: true,
			status:   status,
		}
	}
	return &Rows{lasterr: fmt.Errorf("rethinkdb: Unexpected status code from server: %v", status)}
}

// buildProtobuf converts a query to a protobuf and catches any panics raised
// by the protobuf functions.
func buildProtobuf(ctx context, query Query) (queryProto *p.Query, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = fmt.Errorf("rethinkdb: %v", r)
		}
	}()

	queryProto = query.toProtobuf(ctx)
	return
}

// run is an internal function, shared by Rows iterator and normal the Run()
// call. Runs a protocol buffer formatted query, returns a list of strings and a
// status code.
func (s *Session) run(conn net.Conn, queryProto *p.Query, query Query) (result []string, status p.Response_StatusCode, err error) {
	if debugMode {
		fmt.Printf("rethinkdb: query: %v\n", query)
		fmt.Printf("rethinkdb: queryProto:\n%v", protobufToString(queryProto, 1))
	}

	r, err := executeQuery(conn, queryProto)
	if err != nil {
		return
	}
	if debugMode {
		fmt.Printf("rethinkdb: responseProto:\n%v", protobufToString(r, 1))
	}

	status = r.GetStatusCode()
	switch status {
	case p.Response_SUCCESS_JSON, p.Response_SUCCESS_STREAM, p.Response_SUCCESS_PARTIAL, p.Response_SUCCESS_EMPTY:
		// response is []string, and is empty in the case of SUCCESS_EMPTY
		result = r.Response
	default:
		// some sort of error
		e := Error{Response: r, Query: query}
		switch status {
		case p.Response_RUNTIME_ERROR:
			e.Err = ErrRuntime
		case p.Response_BAD_QUERY:
			e.Err = ErrBadQuery
		case p.Response_BROKEN_CLIENT:
			e.Err = ErrBrokenClient
		default:
			e.Err = fmt.Errorf("rethinkdb: Unexpected status code from server: %v", status)
		}
		err = e
	}
	return
}

func getBacktraceFrames(response *p.Response) []string {
	bt := response.GetBacktrace()
	if bt == nil {
		return nil
	}
	return bt.Frame
}

// Run runs a query using the given session, there is one Run()
// method for each type of query.
func (q MetaQuery) Run(session *Session) *Rows {
	return session.Run(q)
}

func (q WriteQuery) Run(session *Session) *Rows {
	return session.Run(q)
}

func (e Expression) Run(session *Session) *Rows {
	return session.Run(e)
}
