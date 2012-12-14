rethinkgo
=========

[Go language](http://golang.org/) driver for [RethinkDB](http://www.rethinkdb.com/)

***ALPHA VERSION***
===================

This is a work in progress and will undergo a number of changes.  Probably will be mostly done before the end of December 2012.

Current limitations:

    * Only read and administrative queries work, no write queries
    * No global connection object (r.Table()) doesn't work, only (r.DB().Table()), and query.Run() is instead conn.Run(query)
    * Not goroutine safe, each gorouting needs its own connection.  Thank god there's no global connection object.
    * A number of things will be restructured
    * No pretty-printing of queries
    * No docs (besides this one!) or like actual tests

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


