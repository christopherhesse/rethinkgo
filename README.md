rethinkgo
=========

[Go language](http://golang.org/) driver for [RethinkDB](http://www.rethinkdb.com/) made by [Christopher Hesse](http://www.christopherhesse.com/)

****This is a work in progress but is mostly done. There are a few more changes left.  Some constants and types may be renamed.****

Installation
============

    go get -u github.com/christopherhesse/rethinkgo

Example
===================

    package main

    import (
        "fmt"
        r "github.com/christopherhesse/rethinkgo"
    )

    func main() {
        session, _ := r.Connect("localhost:28015", "test")
        rows := r.Expr(1, 2, 3).ArrayToStream().Map(r.Row.Mul(2)).Run(session)
        defer rows.Close()

        var result int
        for rows.Next(&result) {
            fmt.Println("result:", result)
        }

        if err := rows.Err(); err != nil {
            fmt.Println("error:", err)
        }
    }


Overview
========

The Go driver is most similar to the [official Javascript driver](http://www.rethinkdb.com/api/#js).

Most of the functions have all the same names, with a capital first letter.  See [Go Driver Documentation](http://godoc.org/github.com/christopherhesse/rethinkgo) for examples and documentation for each function.

The important types are r.Expression, r.List (used for Arrays, an alias for []interface{}), and r.Map (used for Objects, an alias for map[string]interface{}).

Expr() can take arbitrary structs and uses the "json" module to serialize them.  This means that structs can use the json.Marshaler interface (define a method MarshalJSON() on the struct).  Also, struct fields can also be annotated to specify their JSON equivalents:

    type MyStruct struct {
        MyField int `json:"my_field"`
    }

See the [json docs](http://golang.org/pkg/encoding/json/) for more information.


Differences from official RethinkDB drivers
===========================================

* There is no global implicit connection that stores the last connected server, instead query.Run(*Session) requires a session as its only argument.
* When running queries, getting results is a little different from the more dynamic languages.  .Run(*Session) returns a *Rows iterator object with the following methods that put the response into a variable `dest`, here's when you should use the different methods:
    * You want to iterate through the results of the query individually: rows.Next(&dest)
    * The query always returns a single response: .One(&dest)
    * The query returns a list of responses: .Collect(&dest)
    * The query returns an empty response: .Exec()
* No errors are generated when creating queries, only when running them, so Table(string) returns only an Expression instance, but sess.Run(Query).Err() will tell you if your query could not be serialized for the server.
* There's no r(attributeName) or row[attributeName] function call / item indexing to get attributes of the "current" row or a specific row respectively.  Instead, there is a .Attr() method on the global "Row" object (r.Row) and any row Expressions that can be used to access attributes.  Examples:

        r.Table("marvel").OuterJoin(r.Table("dc"),
            func(marvel, dc r.Expression) interface{} {
                return marvel.Attr("strength").Eq(dc.Attr("strength"))
            })

        r.Table("marvel").Map(r.Row.Attr("strength").Mul(2))

* Go does not have optional args, most optional args are either require or separate methods.
    * A convenience method .GetById(string) has been added for that common case
    * .Atomic(bool) and .Overwrite(bool) are methods on write queries
    * .UseOutdated(bool) is a method on any Table() or other Expression (will apply to all tables already specified)
    * .TableCreate(string) has a variant called TableCreateSpec(TableSpec) which takes a TableSpec instance specifying the parameters for the table
