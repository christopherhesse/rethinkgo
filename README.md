rethinkgo
=========

[Go language](http://golang.org/) driver for [RethinkDB](http://www.rethinkdb.com/) made by [Christopher Hesse](http://www.christopherhesse.com/)

[API Documentation](http://godoc.org/github.com/christopherhesse/rethinkgo)

****This is a work in progress and will undergo a number of changes.  Parity with existing RethinkDB drivers will be mostly done before the end of December 2012.****

Installation
============

    go get github.com/christopherhesse/rethinkgo

Example
===================

    package main

    import (
        "fmt"
        r "github.com/christopherhesse/rethinkgo"
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

* r.Count() is a function, not a constant
* There's a global SetDebug(bool) function to turn on printing of queries, rather than .run(debug=True)
* No errors are generated when creating queries, only when running them, so Table() returns only an Expression instance, but sess.Run(query) returns (*Rows, error)
* There's no r(attributeName) or row[attributeName] function call / item indexing to get attributes of the "current" row or a specific row respectively.  Instead, there is a .Attr() method on the global "Row" object (r.Row) and any row Expressions that can be used to access attributes.  Examples:

        r.Table("marvel").OuterJoin(r.Table("dc"),
            func(marvel, dc r.Expression) interface{} {
                return marvel.Attr("strength").Eq(dc.Attr("strength"))
            })

        r.Table("marvel").Map(r.Row.Attr("strength").Mul(2))

* Go does not have optional args, most optional args are either require or separate methods.
    * A convenience method named ".GetById()" has been added for that common case
    * .Atomic(bool) and .Overwrite(bool) are methods on write queries
    * .UseOutdated(bool) is a method on any Table() or other Expression (will apply to all tables already specified)
    * TableCreate() has a variant called TableCreateSpec(TableSpec) which takes a TableSpec instance specifying the parameters for the table

Current limitations that will gradually be fixed
================================================

* The overall API is fixed because it imitates RethinkDB's [other drivers](http://www.rethinkdb.com/api/), but some specifics of this implementation will change.
* No pretty-printing of queries
* Half-completed docs
* Not goroutine safe, each goroutine needs its own connection.
