package rethinkgo

import (
	"fmt"
	p "github.com/christopherhesse/rethinkgo/query_language"
)

func formatError(message string, response *p.Response) string {
	return fmt.Sprintf("rethinkdb: %v: %v %v", message, response.GetErrorMessage(), getBacktraceFrames(response))
}

func getBacktraceFrames(response *p.Response) []string {
	bt := response.GetBacktrace()
	if bt == nil {
		return nil
	}
	return bt.Frame
}

// ErrBadQuery indicates that the server has told us we have constructed an
// invalid query.
//
// Example usage:
//
//   err := r.Table("heroes").ArrayToStream().ArrayToStream().Run().Err()
type ErrBadQuery struct {
	response *p.Response
}

func (e ErrBadQuery) Error() string {
	return formatError("Server could not make sense of our query", e.response)
}

// ErrRuntime indicates that the server has encountered an error while
// trying to execute our query.
//
// Example usage:
//
//   err := r.Table("table_that_doesnt_exist").Run().Err()
type ErrRuntime struct {
	response *p.Response
}

func (e ErrRuntime) Error() string {
	return formatError("Server could not execute our query", e.response)
}

// ErrBrokenClient means the server believes there's a bug in the client
// library, for instance a malformed protocol buffer.
type ErrBrokenClient struct {
	response *p.Response
}

func (e ErrBrokenClient) Error() string {
	return formatError("Whoops, looks like there's a bug in this client library, please report it at https://github.com/christopherhesse/rethinkgo/issues/new", e.response)
}

// ErrNoSuchRow is returned when there is an empty response from the server and
// .One() is being used.
//
// Example usage:
//
//  var row interface{}
//  err := r.Table("heroes").Get("Apocalypse", "name").Run().One(&row)
type ErrNoSuchRow struct {
	response *p.Response
}

func (e ErrNoSuchRow) Error() string {
	return "rethinkdb: No such row found"
}

// ErrWrongResponseType is returned when .Exec(), .One(). or .Collect() have
// been used, but the expected response type does not match the type we got
// from the server.
//
// Example usage:
//
//  var row []interface{}
//  err := r.Table("heroes").Get("Archangel", "name").Run().Collect(&row)
type ErrWrongResponseType struct {
	response *p.Response
}

func (e ErrWrongResponseType) Error() string {
	return "rethinkdb: Wrong response type, you may have used the wrong one of: .Exec(), .One(), .Collect()"
}
