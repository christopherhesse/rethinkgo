// Package rethinkgo implements the RethinkDB API for Go.  RethinkDB is an open-
// source distributed database in the style of MongoDB.
//
//  http://www.rethinkdb.com/
//
// If you are familiar with the RethinkDB API, this package should be
// straightforward to use, if not, the docs for this package contain plenty of
// examples.  The official RethinkDB API docs are also quite good:
//
//  http://www.rethinkdb.com/docs/
//
// To access a RethinkDB database, you connect to it with the Connect function:
//
//  import r "rethinkdb"
//
//  func main() {
//      db, err := r.Connect("localhost:8080", "<database name>")
//  }
//
// This creates a database session 'db' that may be used to run queries on the
// server.  Queries let you read, insert, update, and delete JSON objects
// ("rows") on the server, as well as manage tables.
//
//  query := r.Table("employees")
//  rows, err := db.Run(query)
//
// If the query was successful, 'rows' is an iterator that can be used to
// iterate over the results.
//
//  for rows.Next() {
//      var row Employee
//      err = rows.Scan(&row)
//      fmt.Println("row:", row)
//  }
//  if rows.Err() != nil {
//      fmt.Println("err:", rows.Err())
//  }
//
// Besides this simple read query, you can run almost arbitrary expressions on
// the server, even Javascript code.  See the rest of these docs for more
// details.
package rethinkdb
