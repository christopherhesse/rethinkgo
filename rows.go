package rethinkgo

import (
	"code.google.com/p/goprotobuf/proto"
	"errors"
	"fmt"
	"reflect"
	p "github.com/christopherhesse/rethinkgo/ql2"
)

// Rows is an iterator to move through the rows returned by the database, call
// rows.Scan(&dest) in a loop to scan a row into the variable `dest`,
// rows.Next() returns false when there is an error or no more rows left.
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
//  err := r.Table("heroes").Get("Omega Red", "name").Run(session).One(&response)
//
// .All(&dest) for a list of results:
//
//  var response []string
//  err := r.Db("marvel").TableList().Run(session).All(&response)
//
// .All() may perform multiple network requests to get all of the results of
// the query.  Use .Limit() if you only need a certain number.
//
// All three of these methods will return errors if used on a query response
// that does not match the expected type (ErrWrongResponseType).
type Rows struct {
	session      *Session
	closed       bool
	buffer       []*p.Datum
	current      *p.Datum
	complete     bool // We have retrieved all the results for a query
	lasterr      error
	token        int64
	responseType p.Response_ResponseType
}

// continueQuery creates a query that will cause this query to continue
func (rows *Rows) continueQuery() error {
	queryProto := &p.Query{
		Type:  p.Query_CONTINUE.Enum(),
		Token: proto.Int64(rows.token),
	}
	buffer, responseType, err := rows.session.conn.executeQuery(queryProto, rows.session.timeout)
	if err != nil {
		return err
	}

	switch responseType {
	case p.Response_SUCCESS_PARTIAL:
		// continuation of a stream of rows
		rows.buffer = buffer
	case p.Response_SUCCESS_SEQUENCE:
		// end of a stream of rows, there's no more after this
		rows.buffer = buffer
		rows.complete = true
	default:
		return fmt.Errorf("rethinkdb: Unexpected response type: %v", responseType)
	}
	return nil
}

// Next moves the iterator forward by one document, returns false if there are
// no more rows or some sort of error has occurred (use .Err() to get the last
// error). `dest` must be passed by reference.
//
// Example usage:
//
//  rows := r.Table("heroes").Run(session)
//  for rows.Next() {
//      var hero interface{}
//      rows.Scan(&hero)
//      fmt.Println("hero:", hero)
//  }
//  if rows.Err() != nil {
//      ...
//  }
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
			rows.closed = true
			return false
		} else {
			// more rows to get, fetch 'em
			err := rows.continueQuery()
			if err != nil {
				rows.lasterr = err
				return false
			}
		}
	}

	if len(rows.buffer) > 0 {
		rows.current = rows.buffer[0]
		rows.buffer = rows.buffer[1:len(rows.buffer)]
	}

	return true
}

// Scan writes the current row into the provided variable, which must be passed
// by reference.
//
// NOTE: Scan uses json.Unmarshal internally and will not clear the destination
// before writing the next row.  Make sure to create a new destination or clear
// it before calling .Scan(&dest).
func (rows *Rows) Scan(dest interface{}) error {
	return datumUnmarshal(rows.current, dest)
}

// Err returns the last error encountered, for example, a network error while
// contacting the database server, or while parsing.
//
// Example usage:
//
//  err := r.Table("heroes").Run(session).Err()
func (rows *Rows) Err() error {
	return rows.lasterr
}

// All fetches all the results from an iterator into a reference to a slice.  It
// may perform multiple network requests to the server until it has retrieved
// all results.
//
// Example usage:
//
//  var result []interface{}
//  err := r.Table("heroes").Run(session).All(&result)
func (rows *Rows) All(slice interface{}) error {
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

	if rows.responseType == p.Response_SUCCESS_PARTIAL || rows.responseType == p.Response_SUCCESS_SEQUENCE {
		// create a new slice to hold the results
		newSliceValue := reflect.MakeSlice(sliceValue.Type(), 0, 0)
		for rows.Next() {
			// create a new element of the kind that the slice holds so we can scan
			// into it
			elemValue := reflect.New(sliceValue.Type().Elem())
			if err := rows.Scan(elemValue.Interface()); err != nil {
				return err
			}
			newSliceValue = reflect.Append(newSliceValue, elemValue.Elem())
		}

		if rows.Err() != nil {
			return rows.Err()
		}

		sliceValue.Set(newSliceValue)
		return nil
	} else if rows.responseType == p.Response_SUCCESS_ATOM {
		// if we got a single datum from the server, try to read it into the slice we got
		if rows.Next() {
			if err := rows.Scan(slicePointerValue.Interface()); err != nil {
				return err
			}
		}

		if rows.Err() != nil {
			return rows.Err()
		}
		return nil
	}
	return ErrWrongResponseType{}
}

// One gets the first result from a query response.
//
// Example usage:
//
//  var result interface{}
//  err := r.Table("villains").Get("Galactus", "name").Run(session).One(&result)
func (rows *Rows) One(row interface{}) error {
	if rows.Err() != nil {
		return rows.Err()
	}

	if rows.responseType != p.Response_SUCCESS_ATOM {
		return ErrWrongResponseType{}
	}

	rows.Next()
	if err := rows.Scan(row); err != nil {
		return err
	}

	return rows.Err()
}

// Exec is for queries for which you wish to ignore the result.  For instance,
// creating a table.
//
// Example usage:
//
//  err := r.TableCreate("villains").Run(session).Exec()
func (rows *Rows) Exec() error {
	if rows.Err() != nil {
		return rows.Err()
	}

	return nil
}
