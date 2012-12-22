package rethinkdb

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/json"
	"errors"
	"fmt"
	p "github.com/christopherhesse/rethinkgo/src/rethinkdb/query_language"
	"io"
	"reflect"
)

// An interator to move through the rows returned by the database, call
// rows.Next() in a loop, and call rows.Scan(&dest) inside the loop to scan
// a row into the variable `dest`
type Rows struct {
	session  *Session
	closed   bool
	buffer   []string
	current  *string
	complete bool
	lasterr  error
	token    int64
	query    RethinkQuery
}

func (rows *Rows) continueQuery() error {
	// create a query that will cause this query to continue
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
			err := rows.continueQuery()
			if err != nil {
				rows.lasterr = err
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
// structure. `dest` must be passed by reference, e.g. rows.Scan(&dest)
func (rows *Rows) Scan(dest interface{}) error {
	if rows.closed {
		return errors.New("rethinkdb: Scan on closed Rows")
	}
	if rows.lasterr != nil {
		return rows.lasterr
	}
	if rows.current == nil {
		return errors.New("rethinkdb: Scan called without calling Next")
	}
	return json.Unmarshal([]byte(*rows.current), dest)
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
// automatically closed when the end of the stream is reached
func (rows *Rows) Close() error {
	if rows.closed {
		return nil
	}
	rows.closed = true
	return nil
}

// Collect all results from the iterator into a reference to a slice
func (rows *Rows) Collect(slice interface{}) error {
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
	for rows.Next() {
		err := rows.Scan(elemValue.Interface())
		if err != nil {
			return err
		}
		newSliceValue = reflect.Append(newSliceValue, elemValue.Elem())
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	sliceValue.Set(newSliceValue)
	return nil
}
