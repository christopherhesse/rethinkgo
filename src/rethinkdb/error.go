package rethinkdb

import (
	"errors"
	"fmt"
	p "github.com/christopherhesse/rethinkgo/src/rethinkdb/query_language"
)

type RethinkError struct {
	Response *p.Response
	Query    RethinkQuery
	Err      error
}

func (e RethinkError) Error() string {
	if e.Response == nil {
		return fmt.Sprintf("rethinkdb: %v", e.Err)
	}
	return fmt.Sprintf("rethinkdb: %v: %v %v %v", e.Err, e.Response.GetErrorMessage(), getBacktraceFrames(e.Response), e.Query)
}

var (
	// ErrRuntime indicates that the server has encountered an error while
	// trying to execute our query. Example:
	//
	//   r.Table("table_that_doesnt_exist")
	ErrRuntime = errors.New("Server could not execute our query")

	// ErrBadQuery indicates that the server has told us we have constructed an
	// invalid query. Example:
	//
	//   r.Table("employees").ArrayToStream().ArrayToStream()
	ErrBadQuery = errors.New("Server could not make sense of our query")

	// ErrNoDb indicates that we used the .Run() method on a query without first
	// connecting to a database. Example:
	//
	//   r.Table("employees").Run()
	ErrNoDb = errors.New("No databases have been connected to yet, you must use r.Connect() to connect to a database before calling query.Run()")

	// ErrBrokenClient means the server believes there's a bug in the client
	// library, for instance a mal-formed protocol buffer or other message.
	ErrBrokenClient = errors.New("Whoops, looks like there's a bug in this client library, please report it at https://github.com/christopherhesse/rethinkgo/issues/new")

	// ErrNoRows is returned when there is an empty response from the server and
	// RunSingle() is being used. Example:
	//
	//  r.Table("employees").Get("totally_not_a_valid_id").RunSingle(&row)
	ErrNoRows = errors.New("No rows returned")
)
