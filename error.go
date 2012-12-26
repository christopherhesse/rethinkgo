package rethinkgo

import (
	"errors"
	"fmt"
	p "github.com/christopherhesse/rethinkgo/query_language"
)

type Error struct {
	Response *p.Response
	Query    Query
	Err      error
}

// Error returns a string representation of the error.
func (e Error) Error() string {
	if e.Response == nil {
		return fmt.Sprintf("rethinkdb: %v", e.Err)
	}
	return fmt.Sprintf("rethinkdb: %v: %v %v %v", e.Err, e.Response.GetErrorMessage(), getBacktraceFrames(e.Response), e.Query)
}

var (
	// ErrRuntime indicates that the server has encountered an error while
	// trying to execute our query.
	//
	//   err := r.Table("table_that_doesnt_exist").Run().Err()
	ErrRuntime = errors.New("Server could not execute our query")

	// ErrBadQuery indicates that the server has told us we have constructed an
	// invalid query.
	//
	//   err := r.Table("work").ArrayToStream().ArrayToStream().Run().Err()
	ErrBadQuery = errors.New("Server could not make sense of our query")

	// ErrNoSession indicates that we used the .Run() method on a query without first
	// connecting to a server.
	//
	//   err := r.Table("drum").Run().Err()
	ErrNoSession = errors.New("No databases have been connected to yet, you must use r.Connect() to connect to a database before calling query.Run()")

	// ErrBrokenClient means the server believes there's a bug in the client
	// library, for instance a mal-formed protocol buffer or other message.
	ErrBrokenClient = errors.New("Whoops, looks like there's a bug in this client library, please report it at https://github.com/christopherhesse/rethinkgo/issues/new")

	// ErrNoRows is returned when there is an empty response from the server and
	// .One() is being used.
	//
	//  err := r.Table("pembroke").Get("totally_not_a_valid_id").Run().One(&row)
	ErrNoRows = errors.New("No rows returned")
)
