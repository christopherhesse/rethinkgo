rethinkgo
=========

[Go language](http://golang.org/) driver for [RethinkDB](http://www.rethinkdb.com/)

***BETA VERSION***
===================

This is a work in progress and will undergo a number of changes.  Probably will be mostly done before the end of December 2012.

Example:

    package main

    import (
        "fmt"
        r "rethinkdb"
    )

    func main() {
        rc, _ := r.Connect("localhost:28015", "test")
        query := r.Expr([]int{1, 2, 3}).ArrayToStream().Map(r.JS(`row*2`))
        rows, _ := rc.Run(query)

        var result int
        for rows.Next() {
            rows.Scan(&result)
            fmt.Println("result:", result)
        }
    }


Overview
========

The Go driver is most similar to the [official Javascript driver](http://www.rethinkdb.com/api/#js).

The important types are r.Expression, []interface{} (used for Arrays), and map[string]interface{} (used for Objects).

Expr() can take arbitrary structs and uses the "json" module to serialize them.  This means that structs can use the json.Marshaler interface (define a method MarshalJSON on the struct).  Struct fields can also be annotated to specify their JSON equivalents:

    type MyStruct struct {
        Field int `json:"myName"`
    }

See the [json docs](http://golang.org/pkg/encoding/json/) for more information.


Differences from official RethinkDB drivers
===========================================

* Go does not have optional args, so all optional args are required for this driver.
    * A convenience method named ".GetById()" has been added for that particular common case
* r.Count() is a function, not a constant
* .GroupBy() only takes a single attribute (for now)
* There's a global SetDebug(bool) function to turn on printing of queries, rather than .run(debug=True)
* Table() does not take a useOutdated boolean argument, instead call .UseOutdated(bool) on the table, e.g. Table("test").UseOutdated(true)
* No errors are generated when creating queries, only when running them, so Table() returns only an Expression type, but rc.Run(query) returns (*Rows, error)

Current limitations that will gradually be fixed
================================================

* No global connection object (r.Table()) doesn't work, only (r.DB().Table()), and query.Run() is instead conn.Run(query)
* The overall API is fixed because it imitates RethinkDB's [other drivers](http://www.rethinkdb.com/api/), but some specifics of this implementation will change.
* No pretty-printing of queries
* No docs (besides this one!) or like actual tests
* Not goroutine safe, each goroutine needs its own connection.  Thank god there's no global connection object.
