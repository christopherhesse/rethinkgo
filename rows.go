package rethinkgo

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/json"
	"errors"
	"fmt"
	p "github.com/christopherhesse/rethinkgo/query_language"
	"io"
	"reflect"
)

// Rows is an interator to move through the rows returned by the database, call
// rows.Next(&dest) in a loop to scan a row into the variable `dest`, returns
// false when there is an error or no more rows left.
//
// There are three methods on the rows object that can be used to avoid
// iterating in this manner. These three methods correspond to the return types
// of a query:
//
// .Exec() for an empty response:
//
//  err := r.Db("marvel").TableCreate("heroes").Exec()
//
// .One(&dest) for a response that always returns a single result:
//
//  var response string
//  err := r.Table("heroes").Get("Omega Red", "name").Run().One(&response)
//
// .Collect(&dest) for a list of results:
//
//  var response []string
//  err := r.Db("marvel").TableList().Run().Collect(&response)
//
// .Collect() may perform multiple network requests to get all of the results of
// the query.  Use .Limit() if you only need a certain number.
//
// All three of these methods will return errors if used on a query response
// that does not match the expected type (ErrWrongResponseType).
type Rows struct {
	session  *Session
	conn     *connection
	closed   bool
	buffer   []string
	current  *string
	complete bool
	lasterr  error
	token    int64
	status   p.Response_StatusCode
}

// continueQuery creates a query that will cause this query to continue
func (rows *Rows) continueQuery() error {
	queryProto := &p.Query{
		Type:  p.Query_CONTINUE.Enum(),
		Token: proto.Int64(rows.token),
	}
	buffer, status, err := rows.conn.executeQuery(queryProto, rows.session.timeout)
	if err != nil {
		return err
	}

	switch status {
	case p.Response_SUCCESS_PARTIAL:
		// continuation of a stream of rows
		rows.buffer = buffer
	case p.Response_SUCCESS_STREAM:
		// end of a stream of rows, there's no more after this
		rows.buffer = buffer
		rows.complete = true
		// since we won't be needing this connection anymore, we can close the
		// iterator, if the user closes it again, it won't hurt it
		rows.Close()
	default:
		return fmt.Errorf("rethinkdb: Unexpected status code: %v", status)
	}
	return nil
}

// Next moves the iterator forward by one document, returns false if there are
// no more rows or some sort of error has occurred (use .Err() to get the last
// error). `dest` must be passed by reference.
//
// Example usage:
//
//  rows := r.Table("heroes").Run()
//  var hero interface{}
//  for rows.Next(&hero) {
//      fmt.Println("hero:", hero)
//  }
//  if rows.Err() != nil {
//      ...
//  }
func (rows *Rows) Next(dest interface{}) bool {
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
			err := rows.continueQuery()
			if err != nil {
				rows.lasterr = err
			}
		}
	}

	if len(rows.buffer) > 0 {
		rows.current = &rows.buffer[0]
		rows.buffer = rows.buffer[1:len(rows.buffer)]
		err := json.Unmarshal([]byte(*rows.current), dest)
		if err != nil {
			rows.lasterr = err
		}
	}

	if rows.lasterr == io.EOF {
		rows.closed = true
	}
	return rows.lasterr == nil
}

// Err returns the last error encountered, for example, a network error while
// contacting the database server, or while parsing JSON.
//
// Example usage:
//
//  err := r.Table("heroes").Run().Err()
func (rows *Rows) Err() error {
	if rows.lasterr == io.EOF {
		// this represents a normal termination of the iterator, so it doesn't really
		// count as an error
		return nil
	}
	return rows.lasterr
}

// Collect gets all results from the iterator into a reference to a slice.  It
// may perform multiple network requests to the server until it has retrieved
// all results.
//
// Example usage:
//
//  var result []interface{}
//  err := r.Table("heroes").Run().Collect(&result)
func (rows *Rows) Collect(slice interface{}) error {
	if rows.Err() != nil {
		return rows.Err()
	}

	slicePointerValue := reflect.ValueOf(slice)
	if slicePointerValue.Kind() != reflect.Ptr {
		return errors.New("rethinkdb: `slice` should probably should be a pointer to a slice")
	}

	sliceValue := slicePointerValue.Elem()
	if sliceValue.Kind() != reflect.Slice {
		return errors.New("rethinkdb: A slice type must be provided")
	}

	if rows.status != p.Response_SUCCESS_PARTIAL && rows.status != p.Response_SUCCESS_STREAM {
		return WrongResponseTypeError{}
	}

	// create a new slice to hold the results
	newSliceValue := reflect.MakeSlice(sliceValue.Type(), 0, 0)
	// create a new element of the kind that the slice holds so we can scan
	// into it
	elemValue := reflect.New(sliceValue.Type().Elem())
	for rows.Next(elemValue.Interface()) {
		if rows.Err() != nil {
			return rows.Err()
		}
		newSliceValue = reflect.Append(newSliceValue, elemValue.Elem())
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	sliceValue.Set(newSliceValue)
	return nil
}

// One gets the first result from a query response.
//
// Example usage:
//
//  var result interface{}
//  err := r.Table("villains").Get("Galactus", "name").Run().One(&result)
func (rows *Rows) One(row interface{}) error {
	if rows.Err() != nil {
		return rows.Err()
	}

	if rows.status != p.Response_SUCCESS_JSON {
		return WrongResponseTypeError{}
	}

	if rows.lasterr == io.EOF {
		return NoSuchRowError{}
	}

	rows.Next(row)

	rows.Close()

	return rows.Err()
}

// Exec is for queries that return no result.  For instance, creating a table.
//
// Example usage:
//
//  err := r.Db("marvel").TableCreate("villains").Run().Exec()
func (rows *Rows) Exec() error {
	if rows.Err() != nil {
		return rows.Err()
	}

	rows.Close()

	if rows.status != p.Response_SUCCESS_EMPTY {
		return WrongResponseTypeError{}
	}

	return nil
}

// Close frees up the connection associated with this iterator, if any.  Just
// use defer rows.Close() after retrieving a Rows iterator.  Not required with
// .Exec(), .One(), or .Collect().
//
// Only stream responses will have an associated connection.
//
// Example usage:
//
//  rows := r.Table("villains").Run()
//  defer rows.Close()
//
//  var result interface{}
//  for rows.Next(&result) {
//      fmt.Println("result:", result)
//  }
func (rows *Rows) Close() (err error) {
	if !rows.closed {
		if rows.conn != nil {
			// if rows.conn is not nil, that means this is a stream response

			// if this Rows iterator was closed before retrieving all results, send a
			// stop query to the server to discard any remaining results
			if !rows.complete {
				queryProto := &p.Query{
					Type:  p.Query_STOP.Enum(),
					Token: proto.Int64(rows.token),
				}
				_, _, err = rows.conn.executeQuery(queryProto, rows.session.timeout)
			}

			// return this connection to the pool
			rows.session.putConn(rows.conn)
		}
		rows.closed = true
	}
	return
}
