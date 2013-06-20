package rethinkgo

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	p "github.com/christopherhesse/rethinkgo/ql2"
	"sync/atomic"
	"time"
)

// Session represents a connection to a server, use it to run queries against a
// database, with either sess.Run(query) or query.Run(session).  Do not share a
// session between goroutines, create a new one for each goroutine.
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

	conn *connection
	closed    bool
}

// Connect creates a new database session.
//
// NOTE: You probably should not share sessions between goroutines.
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

	s.closed = false
	var err error
	s.conn, err = serverConnect(s.address)
	return err
}

// Close closes the session, freeing any associated resources.
//
// Example usage:
//
//  err := sess.Close()
func (s *Session) Close() error {
	if s.closed {
		return nil
	}

	err := s.conn.Close()
	s.closed = true

	return err
}

// SetTimeout causes any future queries that are run on this session to timeout
// after the given duration, returning a timeout error.  Set to zero to disable.
//
// The timeout is global to all queries run on a single Session and does not
// apply to any query currently in progress.
func (s *Session) SetTimeout(timeout time.Duration) {
	s.timeout = timeout
}

// Use changes the default database for a connection.  This is the database that
// will be used when a query is created without an explicit database.  This
// should not be used if the session is shared between goroutines, confusion
// would result.
//
// Example usage:
//
//  sess.Use("dave")
//  rows := r.Table("employees").Run(session) // uses database "dave"
func (s *Session) Use(database string) {
	s.database = database
}

// getToken generates the next query token, used to number requests and match
// responses with requests.
func (s *Session) getToken() int64 {
	return atomic.AddInt64(&s.token, 1)
}

// Run executes a query directly on a specific session and returns an iterator
// that moves through the resulting JSON rows with rows.Next() and
// rows.Scan(&dest). See the documentation for the Rows object for other
// options.
//
// Example usage:
//
//  rows := session.Run(query)
//  for rows.Next() {
//      var row map[string]interface{}
//      rows.Scan(&row)
//      fmt.Println("row:", row)
//  }
//  if rows.Err() {
//      ...
//  }
func (s *Session) Run(query Exp) *Rows {
	queryProto, err := s.getContext().buildProtobuf(query)
	if err != nil {
		return &Rows{lasterr: err}
	}

	queryProto.Token = proto.Int64(s.getToken())
	buffer, responseType, err := s.conn.executeQuery(queryProto, s.timeout)
	if err != nil {
		return &Rows{lasterr: err}
	}

	switch responseType {
	case p.Response_SUCCESS_ATOM:
		// single document (or json) response, return an iterator anyway for
		// consistency of types
		return &Rows{
			buffer:       buffer,
			complete:     true,
			responseType: responseType,
		}
	case p.Response_SUCCESS_PARTIAL:
		// beginning of stream of rows, there are more results available from the
		// server than the ones we just received, so save the session we used in
		// case the user wants more
		return &Rows{
			session:      s,
			buffer:       buffer,
			token:        queryProto.GetToken(),
			responseType: responseType,
		}
	case p.Response_SUCCESS_SEQUENCE:
		// end of a stream of rows, since we got this on the initial query this means
		// that we got a stream response, but the number of results was less than the
		// number required to break the response into chunks. we can just return all
		// the results in one go, as this is the only response
		return &Rows{
			buffer:       buffer,
			complete:     true,
			responseType: responseType,
		}
	}
	return &Rows{lasterr: fmt.Errorf("rethinkdb: Unexpected response type from server: %v", responseType)}
}

func (s *Session) getContext() context {
	return context{databaseName: s.database, atomic: true}
}

// Run runs a query using the given session, there is one Run()
// method for each type of query.
func (e Exp) Run(session *Session) *Rows {
	return session.Run(e)
}
