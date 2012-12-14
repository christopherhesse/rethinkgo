package rethinkdb

import (
	"fmt"
	"testing"
)

func TestQueries(t *testing.T) {
	SetDebug(true)

	fmt.Println("start")
	rc, err := Connect("localhost:28015", "test")
	fmt.Println("connect:", err)
	err = rc.Close()
	fmt.Println("close:", err)
	err = rc.Reconnect()
	fmt.Println("reconnect:", err)

	// Make sure errors work
	assertError(rc, DB("test").TableDrop("testtable"))
	assertError(rc, Err("test"))

	queries := []RethinkQuery{
		DBCreate("testdb"),
		DBList(),
		DBDrop("testdb"),
		DB("test").TableCreate("testtable"),
		DB("test").TableList(),
		DB("test").TableDrop("testtable"),
		DB("test").Table("characters").Map(JS(`[row.awesomeness * 2, 3]`)),
		DB("test").Table("characters").Map(
			func(row Expression) Expression {
				return row.Attr("awesomeness").Mul(2)
			}),
		Expr(2).Mul(2),
		Expr([]int{1, 2, 3}).ArrayToStream().Map(JS(`row*2`)),
		DB("test").Table("characters").Get("f001af8b-7d11-45a4-a268-a073ad4756ff", "id"),
		DB("test").Table("characters").GetById("f001af8b-7d11-45a4-a268-a073ad4756ff"),
		DB("test").Table("characters").GetById(JS(`"f001af8b-7d11-45a4-a268-a073ad4756ff"`)),
		JS("{'name':2}").Attr("name"),
		DB("test").Table("characters").GetById("f001af8b-7d11-45a4-a268-a073ad4756ff").Contains("show"),
		DB("test").Table("characters").GetById("f001af8b-7d11-45a4-a268-a073ad4756ff").Pick("show"),
		Expr(nil),
		Expr([]int{1, 2, 3}).ArrayToStream().Nth(2),
		Expr([]int{1, 2, 3}).ArrayToStream().Slice(3, nil),
		DB("test").Table("characters").BetweenIds("199bbc8c-d48e-470a-b79b-b853e0881099", "c384aec7-53ff-4c68-ac9e-233317af44f4"),
		DB("test").Table("characters").Filter(
			func(row Expression) Expression {
				return Attr("name").Eq("William Adama")
			}),
		DB("test").Table("characters").Filter(map[string]interface{}{"name": "William Adama", "show": "Battlestar Galactica"}),
		DB("test").Table("characters").OrderBy("name", Desc("awesomeness")),
		DB("test").Table("characters").Map(
			func(row Expression) Expression {
				return Attr("awesomeness")
			}).Reduce(0,
			func(acc, row Expression) Expression {
				return acc.Add(row)
			}),
		DB("test").Table("characters").GroupedMapReduce(
			func(row Expression) Expression {
				return row.Attr("awesomeness")
			},
			func(row Expression) Expression {
				return row.Attr("awesomeness")
			},
			1,
			func(acc, row Expression) Expression {
				return acc.Add(row)
			},
			// TODO: see if this can be made to work
			// "names:",
			// JS("acc + row.substring(0,5)"),
		),
	}

	for _, query := range queries {
		result, err := rc.Run(query)
		fmt.Println("query struct:", query, "result:", result, "err:", err)
		if err != nil {
			panic(err)
		}
	}
}

func assertError(rc *RethinkConnection, q RethinkQuery) {
	_, err := rc.Run(q)
	e := err.(RuntimeError)
	fmt.Println("err:", e)
}
