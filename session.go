package rethinkgo

import (
	"code.google.com/p/goprotobuf/proto"
	"errors"
	"fmt"
	p "github.com/christopherhesse/rethinkgo/query_language"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// maximum number of connections to a server to keep laying around, should add
// a function to change this if there is any demand
var maxIdleConnections int = 5

// Session represents a connection to a server, use it to run queries against a
// database, with either sess.Run(query) or query.Run() (uses the most
// recently-created session).  It is safe to use from multiple goroutines.
type Session struct {
	// current query identifier, just needs to be unique for each query, so we
	// can match queries with responses, e.g. 4782371
	token int64
	// address of server, e.g. "localhost:28015"
	address string
	// database to use if no database is specified in query, e.g. "test"
	database string
	// maximum duration of a single query
	timeout time.Duration

	// protects idleConns and closed, because this lock is here, the session
	// should not be copied according to the "sync" module
	mutex     sync.Mutex
	idleConns []*connection
	closed    bool
}

// Query is the interface for queries that can be .Run(), this includes
// Exp (run as a read query), MetaQuery, and WriteQuery. Methods that
// generate a query are generally located on Exp objects.
type Query interface {
	toProtobuf(context) *p.Query // will panic on errors
	Check(*Session) error
	Run(*Session) *Rows
}

// Connect creates a new database session.
//
// Example usage:
//
//  sess, err := r.Connect("localhost:28015", "test")
func Connect(address, database string) (*Session, error) {
	s := &Session{address: address, database: database, closed: true}

	err := s.Reconnect()

	if err != nil {
		return nil, err
	}

	return s, nil
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

	// create a connection to make sure the server works, then immediately put it
	// in the idle connection pool
	conn, err := s.getConn()
	if err != nil {
		return err
	}

	s.putConn(conn)

	return nil
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

// SetTimeout causes any future queries that are run on this session to timeout
// after the given duration, returning a timeout error.  Set to zero to disable.
//
// The timeout is global to all queries run on a single Session and does not
// apply to queries currently in progress.  The timeout does not cover the
// time taken to connect to the server in the case that there is no idle
// connection available.
//
// If a timeout occurs, the individual connection handling that query will be
// closed instead of being returned to the connection pool.
func (s *Session) SetTimeout(timeout time.Duration) {
	s.timeout = timeout
}

// return a connection from the free connections list if available, otherwise,
// create a new connection
func (s *Session) getConn() (*connection, error) {
	s.mutex.Lock()
	if s.closed {
		s.mutex.Unlock()
		return nil, errors.New("rethinkdb: session is closed")
	}
	if n := len(s.idleConns); n > 0 {
		// grab from end of slice so that underlying array does not need to be
		// resized when appending idle connections later
		conn := s.idleConns[n-1]
		s.idleConns = s.idleConns[:n-1]
		s.mutex.Unlock()
		return conn, nil
	}
	s.mutex.Unlock()

	conn, err := serverConnect(s.address)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// return a connection to the free list, or close it if we already have enough
func (s *Session) putConn(conn *connection) {
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
	queryProto, err := s.getContext().buildProtobuf(query)
	if err != nil {
		return &Rows{lasterr: err}
	}

	queryProto.Token = proto.Int64(s.getToken())

	conn, err := s.getConn()
	if err != nil {
		return &Rows{lasterr: err}
	}

	buffer, status, err := conn.executeQuery(queryProto, s.timeout)
	if err != nil {
		// see if we got a timeout error, close the connection if we did, since
		// the connection may not be idle for quite some time and we don't
		// want to try multiplexing queries over a rethinkdb connection
		//
		// judging from rethinkdb's CPU usage, this won't actually terminate the
		// query, see https://github.com/rethinkdb/rethinkdb/issues/372
		netErr, ok := err.(net.Error)
		if ok && netErr.Timeout() {
			conn.Close()
		} else {
			s.putConn(conn)
		}
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
			conn:     conn,
			buffer:   buffer,
			complete: false,
			token:    queryProto.GetToken(),
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

func (s *Session) getContext() context {
	return context{databaseName: s.database}
}

// Run runs a query using the given session, there is one Run()
// method for each type of query.
func (e Exp) Run(session *Session) *Rows {
	return session.Run(e)
}

func (q MetaQuery) Run(session *Session) *Rows {
	return session.Run(q)
}

func (q WriteQuery) Run(session *Session) *Rows {
	return session.Run(q)
}
