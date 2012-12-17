package rethinkdb

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	p "rethinkdb/query_language"
)

var DEBUG bool = false

const CLIENT_HELLO uint32 = 0xaf61ba35

type RethinkConnection struct {
	conn     net.Conn
	token    int64
	address  string
	database string
	closed   bool
}

func SetDebug(debug bool) {
	DEBUG = debug
}

type RethinkQuery interface {
	buildProtobuf() (*p.Query, error)
}

// Write a byte array to the stream preceeded by the length in bytes
func writeMessage(conn net.Conn, data []byte) (err error) {
	messageLength := uint32(len(data))
	if err = binary.Write(conn, binary.LittleEndian, messageLength); err != nil {
		return
	}

	_, err = conn.Write(data)
	if err != nil {
		return
	}
	return
}

// Write a protobuf message to the stream
func writeQuery(conn net.Conn, protobuf *p.Query) error {
	data, err := proto.Marshal(protobuf)
	if err != nil {
		return fmt.Errorf("Could not marshal protocol buffer: %v, %v", protobuf, err)
	}

	return writeMessage(conn, data)
}

// Read a message from the stream, which is a length then a serialized protocol
// buffer
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

// Read a protobuf message from a stream
func readResponse(conn net.Conn) (*p.Response, error) {
	data, err := readMessage(conn)
	if err != nil {
		return nil, err
	}
	response := &p.Response{}
	err = proto.Unmarshal(data, response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

// Create a new connection to the database server, e.g. Connect("localhost:28015", "test")
func Connect(address, database string) (*RethinkConnection, error) {
	rc := &RethinkConnection{address: address, database: database, closed: true}
	if err := rc.Reconnect(); err != nil {
		return nil, err
	}
	return rc, nil
}

// Close an open connection
func (rc *RethinkConnection) Close() error {
	if rc.closed {
		return nil
	}
	rc.closed = true
	return rc.conn.Close()
}

// Close and re-open a connection, cancels any outstanding requests
func (rc *RethinkConnection) Reconnect() error {
	if err := rc.Close(); err != nil {
		return err
	}
	conn, err := net.Dial("tcp", rc.address)
	if err != nil {
		return err
	}
	rc.conn = conn
	rc.closed = false
	if err := binary.Write(conn, binary.LittleEndian, CLIENT_HELLO); err != nil {
		return err
	}
	return nil
}

// Switch to another database
func (rc *RethinkConnection) Use(database string) {
	rc.database = database
}

// Get the next token, used to number requests and match responses with requests
func (rc *RethinkConnection) getToken() int64 {
	token := rc.token
	rc.token += 1
	return token
}

// Execute a single query, get the raw response
func (rc *RethinkConnection) executeQuery(protobuf *p.Query) (response *p.Response, err error) {
	if err = writeQuery(rc.conn, protobuf); err != nil {
		return
	}

	for {
		response, err = readResponse(rc.conn)
		if err != nil {
			return
		}

		if *response.Token == *protobuf.Token {
			break
		} else if *response.Token > *protobuf.Token {
			return nil, errors.New("The server returned a response for a protobuf that was not submitted by us")
		}
	}
	return
}

// An interator to move through the rows returned by the database
type Rows struct {
	rc       *RethinkConnection
	closed   bool
	buffer   []string
	current  *string
	complete bool
	lasterr  error
	token    int64
	query    RethinkQuery
}

// Start iterator or move it forward by one document, returns true if there
// are more rows, false if there are no more rows or some sort of
// error has occurred (use Err() to get the last error)
func (rows *Rows) Next() bool {
	if rows.closed {
		return false
	}
	if rows.lasterr != nil {
		return false
	}
	if len(rows.buffer) == 0 {
		// we're out of results, may need to fetch some more
		if rows.complete {
			// no more rows left to fetch
			rows.lasterr = io.EOF
		} else {
			// more rows to get, fetch 'em
			// create a query that will cause this query to continue
			querybuf := &p.Query{
				Type:  p.Query_CONTINUE.Enum(),
				Token: proto.Int64(rows.token),
			}
			buffer, status, err := rows.rc.run(querybuf, rows.query)
			if err != nil {
				rows.lasterr = err
				return false
			}

			switch status {
			case p.Response_SUCCESS_PARTIAL:
				// continuation of a stream of rows
				rows.buffer = buffer
			case p.Response_SUCCESS_STREAM:
				// end of a stream of rows, there's no more after this
				rows.buffer = buffer
				rows.complete = true
			default:
				rows.lasterr = fmt.Errorf("rethinkdb: Unexpected status code: %v", status)
				return false
			}
		}
	} else {
		rows.current = &rows.buffer[0]
		rows.buffer = rows.buffer[1:len(rows.buffer)]
	}
	if rows.lasterr == io.EOF {
		rows.Close()
	}
	return rows.lasterr == nil
}

// Take the current document in the result set and put it into the provided
// structure
func (rows *Rows) Scan(row interface{}) error {
	if rows.closed {
		return errors.New("rethinkdb: Scan on closed Rows")
	}
	if rows.lasterr != nil {
		return rows.lasterr
	}
	if rows.current == nil {
		return errors.New("rethinkdb: Scan called without calling Next")
	}
	return json.Unmarshal([]byte(*rows.current), row)
}

// Returns the last error encountered during iteration, for example, a network
// error while contacting the database server
func (rows *Rows) Err() error {
	if rows.lasterr == io.EOF {
		// this represents a normal termination of the iterator, so it doesn't really
		// count as an error
		return nil
	}
	return rows.lasterr
}

// Close the iterator, freeing any resources associated with it, iterators are
// automatically closed when the end is reached
func (rows *Rows) Close() error {
	if rows.closed {
		return nil
	}
	rows.closed = true
	return nil
}

// Run a query directly on a connection, returns an iterator through the
// resulting JSON strings
func (rc *RethinkConnection) Run(query RethinkQuery) (rows *Rows, err error) {
	querybuf, err := query.buildProtobuf()
	if err != nil {
		return
	}
	querybuf.Token = proto.Int64(rc.getToken())

	buffer, status, err := rc.run(querybuf, query)
	if err != nil {
		return
	}

	switch status {
	case p.Response_SUCCESS_JSON:
		// single document (or json) response, return an iterator anyway for
		// consistency of types
		rows = &Rows{
			buffer:   buffer,
			complete: true,
		}
	case p.Response_SUCCESS_PARTIAL:
		// beginning of stream of rows
		rows = &Rows{
			rc:       rc,
			buffer:   buffer,
			complete: false,
			token:    *querybuf.Token,
			query:    query,
		}
	case p.Response_SUCCESS_STREAM:
		// end of a stream of rows, since we got this on the initial query
		// we can just return all the responses in one go
		rows = &Rows{
			buffer:   buffer,
			complete: true,
		}
	case p.Response_SUCCESS_EMPTY:
		// nothing to do here
	}
	return
}

// An interator to move through the rows returned by the database
type Document struct {
	current *string
}

func (rc *RethinkConnection) RunSingle(query RethinkQuery) (document *Document, err error) {
	rows, err := rc.Run(query)
	if err != nil {
		return
	}
	document = &Document{
		current: &rows.buffer[0],
	}
	return
}

func (d *Document) Scan(row interface{}) (err error) {
	return json.Unmarshal([]byte(*d.current), row)
}

type RuntimeError struct {
	response *p.Response
	query    RethinkQuery
}

func (e RuntimeError) Error() string {
	return fmt.Sprintf("rethinkdb: Error executing query: %v %v %v", e.response.GetErrorMessage(), getBacktraceFrames(e.response), e.query)
}

type BadQuery struct {
	response *p.Response
	query    RethinkQuery
}

func (e BadQuery) Error() string {
	return fmt.Sprintf("rethinkdb: Bad query: %v %v %v", e.response.GetErrorMessage(), getBacktraceFrames(e.response), e.query)
}

// Internal run function, shared by Rows iterator and normal Run() call
func (rc *RethinkConnection) run(querybuf *p.Query, query RethinkQuery) (result []string, status p.Response_StatusCode, err error) {
	if DEBUG {
		fmt.Printf("rethinkdb: query:\n%v", protobufToString(querybuf, 1))
	}

	r, err := rc.executeQuery(querybuf)
	if err != nil {
		return
	}
	if DEBUG {
		fmt.Printf("rethinkdb: response:\n%v", protobufToString(r, 1))
	}

	status = r.GetStatusCode()
	switch status {
	case p.Response_SUCCESS_JSON:
		fallthrough
	case p.Response_SUCCESS_STREAM:
		fallthrough
	case p.Response_SUCCESS_PARTIAL:
		result = r.Response
	case p.Response_SUCCESS_EMPTY:
		// nothing to do here, we'll end up returning a nil result
	case p.Response_RUNTIME_ERROR:
		err = RuntimeError{r, query}
	case p.Response_BAD_QUERY:
		err = BadQuery{r, query}
	case p.Response_BROKEN_CLIENT:
		err = errors.New("rethinkdb: Broken client: server rejected our protocol buffer as malformed, this client library most likely contains a bug")
	default:
		err = fmt.Errorf("rethinkdb: Got unexpected status code from server: %v", r.StatusCode)
	}
	return
}

func getBacktraceFrames(r *p.Response) []string {
	bt := r.GetBacktrace()
	if bt == nil {
		return nil
	}
	return bt.Frame
}
