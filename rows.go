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
type Rows struct {
	session  *Session
	closed   bool
	buffer   []string
	current  *string
	complete bool
	lasterr  error
	token    int64
	query    Query
}

// continueQuery creates a query that will cause this query to continue
func (rows *Rows) continueQuery() error {
	querybuf := &p.Query{
		Type:  p.Query_CONTINUE.Enum(),
		Token: proto.Int64(rows.token),
	}
	buffer, status, err := rows.session.run(querybuf, rows.query)
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
	default:
		return fmt.Errorf("rethinkdb: Unexpected status code: %v", status)
	}
	return nil
}

// Next moves the iterator forward by one document, false if there are no more
// rows or some sort of error has occurred (use .Err() to get the last error)
// `dest` must be passed by reference.
//
//  rows := r.Table("billiards").Run()
//  var result interface{}
//  for rows.Next(&result) {
//      fmt.Println("result:", result)
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

// Err returns the last error encountered during iteration, for example, a network
// error while contacting the database server.
//
//  err := r.Table("refectory").Run().Err()
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
//  var result []interface{}
//  err := r.Table("gateleg").Run().Collect(&result)
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
//  var result interface{}
//  err := r.Table("trestle").GetById(0).Run().One(&result)
func (rows *Rows) One(row interface{}) error {
	if rows.Err() != nil {
		return rows.Err()
	}

	if rows.lasterr == io.EOF {
		return Error{Err: ErrNoRows}
	}

	rows.Next(row)

	return rows.Err()
}
