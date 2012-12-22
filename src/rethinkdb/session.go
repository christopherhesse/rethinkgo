package rethinkdb

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"errors"
	"fmt"
	p "github.com/christopherhesse/rethinkgo/query_language"
	"io"
	"net"
)

var debugMode bool = false
var LastSession *Session

// Session represents a connection to a server, use it to run queries against a
// database, with either sess.Run(query) or query.Run() which uses the most
// recently-created session.
type Session struct {
	conn     net.Conn
	token    int64
	address  string
	database string
	closed   bool
}

// SetDebug causes all queries send to the server and responses received to be
// printed to stdout in raw form.
func SetDebug(debug bool) {
	debugMode = debug
}

// writeMessage writes a byte array to the stream preceeded by the length in
// bytes
func writeMessage(conn net.Conn, data []byte) error {
	messageLength := uint32(len(data))
	if err := binary.Write(conn, binary.LittleEndian, messageLength); err != nil {
		return err
	}

	_, err := conn.Write(data)
	return err
}

// writeQuery writes a protobuf message to the stream
func writeQuery(conn net.Conn, protobuf *p.Query) error {
	data, err := proto.Marshal(protobuf)
	if err != nil {
		return fmt.Errorf("rethinkdb: Could not marshal protocol buffer: %v, %v", protobuf, err)
	}

	return writeMessage(conn, data)
}

// readMessage reads a single message from the stream, which is a length then a
// serialized protocol buffer
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
func readResponse(conn net.Conn) (response *p.Response, err error) {
	data, err := readMessage(conn)
	if err != nil {
		return
	}
	response = &p.Response{}
	err = proto.Unmarshal(data, response)
	return
}

// Connect create a new connection to the database.
//
//  Connect("localhost:28015", "test")
func Connect(address, database string) (s *Session, err error) {
	s = &Session{address: address, database: database, closed: true}
	err = s.Reconnect()
	if err != nil {
		return
	}
	LastSession = s
	return
}

// Close closes the database, freeing any associated resources.
func (s *Session) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.conn.Close()
}

// Reconnect closes and re-open a connection, canceling any outstanding requests
func (s *Session) Reconnect() error {
	const clientHello uint32 = 0xaf61ba35

	if err := s.Close(); err != nil {
		return err
	}
	conn, err := net.Dial("tcp", s.address)
	if err != nil {
		return err
	}
	s.conn = conn
	s.closed = false
	if err := binary.Write(conn, binary.LittleEndian, clientHello); err != nil {
		return err
	}
	return nil
}

// Use switches to another database on the same server
func (s *Session) Use(database string) {
	s.database = database
}

// getToken generates the next query token, used to number requests and match
// responses with requests
func (s *Session) getToken() int64 {
	token := s.token
	s.token += 1
	return token
}

// executeQuery sends a single query to the server and retrieves the raw
// response
func (s *Session) executeQuery(protobuf *p.Query) (response *p.Response, err error) {
	if err = writeQuery(s.conn, protobuf); err != nil {
		return
	}

	for {
		response, err = readResponse(s.conn)
		if err != nil {
			return
		}

		if response.GetToken() == protobuf.GetToken() {
			break
		} else if response.GetToken() > protobuf.GetToken() {
			return nil, errors.New("rethinkdb: The server returned a response for a protobuf that was not submitted by us")
		}
	}
	return
}

// Run executes a query directly on a connection, returns an iterator that moves
// through the resulting JSON rows with rows.Next() and rows.Scan(&dest).
//
//  rows, err := db.Run(query)
//  defer rows.Close()  TODO: is this important?
//  for rows.Next() {
//      var row map[string]interface{}
//      err = rows.Scan(&row)
//  }
//  if rows.Err() {
//      ...
//  }
func (s *Session) Run(query RethinkQuery) (rows *Rows, err error) {
	ctx := context{databaseName: s.database}
	querybuf, err := query.buildProtobuf(ctx)
	if err != nil {
		return
	}

	querybuf.Token = proto.Int64(s.getToken())

	buffer, status, err := s.run(querybuf, query)
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
			session:  s,
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

// RunSingle runs a query and scans the first result into the provided variable
func (s *Session) RunSingle(query RethinkQuery, row interface{}) error {
	rows, err := s.Run(query)
	if err != nil {
		return err
	}

	if rows.Next() {
		err = rows.Scan(row)
		if err != nil {
			if err == io.EOF {
				return RethinkError{Err: ErrNoRows}
			} else {
				return err
			}
		}
	}

	return rows.Err()
}

// Internal run function, shared by Rows iterator and normal Run() call
// Runs a protocol buffer formatted query, returns a list of strings and
// a status code.
func (s *Session) run(querybuf *p.Query, query RethinkQuery) (result []string, status p.Response_StatusCode, err error) {
	if debugMode {
		fmt.Printf("rethinkdb: query:\n%v", protobufToString(querybuf, 1))
	}

	r, err := s.executeQuery(querybuf)
	if err != nil {
		return
	}
	if debugMode {
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
	default:
		// some sort of error
		e := RethinkError{Response: r, Query: query}
		switch status {
		case p.Response_RUNTIME_ERROR:
			e.Err = ErrRuntime
		case p.Response_BAD_QUERY:
			e.Err = ErrBadQuery
		case p.Response_BROKEN_CLIENT:
			e.Err = ErrBrokenClient
		default:
			e.Err = fmt.Errorf("Unexpected status code from server: %v", r.StatusCode)
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

// RethinkQuery is an interface that all queries must implement to be sent to
// the database.
type RethinkQuery interface {
	buildProtobuf(context) (*p.Query, error)
}

func runLastSession(query RethinkQuery) (*Rows, error) {
	if LastSession == nil {
		return nil, RethinkError{Err: ErrNoDb}
	}
	return LastSession.Run(query)
}

func runSingleLastSession(query RethinkQuery, row interface{}) error {
	if LastSession == nil {
		return RethinkError{Err: ErrNoDb}
	}
	return LastSession.RunSingle(query, row)
}

func (q Query) Run() (*Rows, error) {
	return runLastSession(q)
}

func (q Query) RunSingle(row interface{}) error {
	return runSingleLastSession(q, row)
}

func (e Expression) Run() (*Rows, error) {
	return runLastSession(e)
}

func (e Expression) RunSingle(row interface{}) error {
	return runSingleLastSession(e, row)
}
