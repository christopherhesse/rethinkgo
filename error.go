package rethinkgo

import (
	"fmt"
	p "github.com/christopherhesse/rethinkgo/ql2"
)

func formatError(message string, response *p.Response) string {
	datums := response.GetResponse()
	var responseString string
	if len(datums) == 1 {
		datumUnmarshal(datums[0], &responseString)
	}

	if responseString == "" {
		responseString = fmt.Sprintf("%v", datums)
	}
	return fmt.Sprintf("rethinkdb: %v: %v", message, responseString)
}

func getBacktraceFrames(response *p.Response) []string {
	bt := response.GetBacktrace()
	if bt == nil {
		return nil
	}
	frames := []string{}
	for _, frame := range bt.GetFrames() {
		frames = append(frames, frame.String())
	}
	return frames
}

// ErrBadQuery indicates that the server has told us we have constructed an
// invalid query.
//
// Example usage:
//
//   err := r.Table("heroes").ArrayToStream().ArrayToStream().Run(session).Err()
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
//   err := r.Table("table_that_doesnt_exist").Run(session).Err()
//   err := r.RuntimeError("error time!").Run(session).Err()
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

// ErrWrongResponseType is returned when .Exec(), .One(). or .All() have
// been used, but the expected response type does not match the type we got
// from the server.
//
// Example usage:
//
//  var row []interface{}
//  err := r.Table("heroes").Get("Archangel", "name").Run(session).All(&row)
type ErrWrongResponseType struct {
	response *p.Response
}

func (e ErrWrongResponseType) Error() string {
	return "rethinkdb: Wrong response type, you may have used the wrong one of: .Exec(), .One(), .All()"
}
