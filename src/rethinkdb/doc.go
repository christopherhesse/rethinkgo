// Package rethinkgo implements the RethinkDB API for Go.  RethinkDB is an
// open-source distributed database in the style of MongoDB.
//
//  http://www.rethinkdb.com/
//
// If you haven't tried it out, give it a try, it takes about a minute to setup
// and has a sweet web console.
//
//  http://www.rethinkdb.com/docs/guides/quickstart/
//
// If you are familiar with the RethinkDB API, this package should be
// straightforward to use.  If not, the docs for this package contain plenty of
// examples, but you may want to look through the RethinkDB tutorials:
//
//  http://www.rethinkdb.com/docs/
//
// Example usage:
//
//  import r "github.com/christopherhesse/rethinkgo/src/rethinkdb"
//
//  func main() {
//      // To access a RethinkDB database, you connect to it with the Connect function
//      sess, err := r.Connect("localhost:8080", "<database name>")
//
//      // This creates a database session 'sess' that may be used to run
//      // queries on the server.  Queries let you read, insert, update, and
//      // delete JSON objects ("rows") on the server, as well as manage tables.
//
//      query := r.Table("employees")
//      rows, err := sess.Run(query)
//
//      // If the query was successful, 'rows' is an iterator that can be used to
//      // iterate over the results.
//
//      for rows.Next() {
//          var row Employee
//          err = rows.Scan(&row)
//          fmt.Println("row:", row)
//      }
//      if rows.Err() != nil {
//          fmt.Println("err:", rows.Err())
//      }
//  }
//
// Besides this simple read query, you can run almost arbitrary expressions on
// the server, even Javascript code.  See the rest of these docs for more
// details.
package rethinkdb
