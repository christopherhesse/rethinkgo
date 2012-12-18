package rethinkdb

import (
	"fmt"
	"testing"
)

type Hero struct {
	Name     string `json:"name"`
	Strength int    `json:"strength"`
}

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
	assertError(rc, Error("test"))

	dcCharacter := Hero{Name: "Bruce Wayne", Strength: 5}

	characters := DB("test").Table("characters")
	queries := []RethinkQuery{
		DBCreate("testdb"),
		DBList(),
		DBDrop("testdb"),
		DB("test").TableCreate("testtable"),
		DB("test").TableList(),
		DB("test").TableDrop("testtable"),
		characters.Map(
			func(row Expression) interface{} {
				return row.Attr("awesomeness").Mul(2)
			}),
		Expr(2).Mul(2),
		characters.Get("f001af8b-7d11-45a4-a268-a073ad4756ff", "id"),
		characters.GetById("f001af8b-7d11-45a4-a268-a073ad4756ff"),
		characters.GetById(JS(`"f001af8b-7d11-45a4-a268-a073ad4756ff"`)),
		JS("{'name':2}").Attr("name"),
		characters.GetById("f001af8b-7d11-45a4-a268-a073ad4756ff").Contains("show"),
		characters.GetById("f001af8b-7d11-45a4-a268-a073ad4756ff").Pick("show"),
		Expr(nil),
		Expr([]int{1, 2, 3}).ArrayToStream().Nth(2),
		Expr([]int{1, 2, 3}).ArrayToStream().Slice(3, nil),
		characters.BetweenIds("199bbc8c-d48e-470a-b79b-b853e0881099", "c384aec7-53ff-4c68-ac9e-233317af44f4"),
		characters.Filter(
			func(row Expression) interface{} {
				return Attr("name").Eq("William Adama")
			}),
		characters.Filter(map[string]interface{}{"name": "William Adama", "show": "Battlestar Galactica"}),
		characters.OrderBy("name", Desc("awesomeness")),
		characters.Map(
			func(row Expression) interface{} {
				return Attr("awesomeness")
			}).Reduce(0,
			func(acc, row Expression) interface{} {
				return acc.Add(row)
			}),
		characters.GroupedMapReduce(
			func(row Expression) interface{} {
				return row.Attr("awesomeness")
			},
			func(row Expression) interface{} {
				return row.Attr("awesomeness")
			},
			0,
			func(acc, row Expression) interface{} {
				return acc.Add(row)
			},
		),
		characters.UseOutdated(true),
		characters.Pluck("name", "show"),
		characters.GroupBy("awesomeness", Sum("awesomeness")),
		characters.GroupedMapReduce(
			func(row Expression) Expression {
				return row.Attr("awesomeness")
			},
			func(row Expression) Expression {
				return row.Attr("name")
			},
			"",
			JS(`acc + row`),
		),
		characters.Map(JS(`[row.awesomeness * 2, 3]`)),
		// Expr(func() interface{} { return map[string]interface{}{"name": 3} }).Attr("name"),
		Expr([]int{1, 2, 3}).ArrayToStream().Map(JS(`row*2`)),
		DB("test").Table("marvel").OuterJoin(DB("test").Table("dc"),
			func(marvel, dc Expression) interface{} {
				return marvel.Attr("strength").Eq(dc.Attr("strength"))
			}),
		DB("test").Table("marvel").EqJoin("id", DB("test").Table("dc"), "id"),
		Expr([]int{1, 2, 3}).Skip(1).Count(),
		Expr([]int{1, 2, 2, 3, 2, 2}).ArrayToStream().Distinct(),
		DB("test").Table("dc").Insert(map[string]interface{}{"name": "Unknown", "strength": 1}),
		DB("test").Table("dc").Insert(dcCharacter),
		DB("test").Table("marvel").Update(map[string]interface{}{"age": 31}),
		DB("test").Table("marvel").GetById("e62a977a-5f03-4f86-95f6-1fc59d10459d").Update(map[string]interface{}{"age": 29}),
		DB("test").Table("marvel").GetById("e62a977a-5f03-4f86-95f6-1fc59d10459d").Replace(map[string]interface{}{"id": "e62a977a-5f03-4f86-95f6-1fc59d10459d", "name": "Iron Man", "age": 99, "strength": 8}),
		DB("test").Table("marvel").Filter(map[string]interface{}{"name": "Darkhawk"}).Replace(map[string]interface{}{"id": "f4faf171-9947-4108-9ae2-3f77cd53d012", "name": "Darkhawk", "age": 999, "strength": 9}),
		// DB("test").Table("dc").GetById("aedfebe7-a60a-4b73-8ecd-680dd4b1ac23").Delete(),
		// DB("test").Table("dc").Filter(map[string]interface{}{"name": "Unknown"}).Delete(),
		DB("test").Table("marvel2").Delete(),
		DB("test").Table("marvel").Map(Row.Attr("strength").Mul(2)),
		DB("test").Table("marvel").ForEach(func(row Expression) RethinkQuery {
			return DB("test").Table("marvel2").Insert(row)
		}),
	}

	for _, query := range queries {
		result, err := rc.Run(query)
		fmt.Println("query struct:", query, "result:", result, "err:", err)
		if err != nil {
			panic(err)
		}
	}

	// result, err := rc.RunSingle()
	// var response InsertResponse
	// result.Scan(&response)
	// fmt.Println("insert:", response, err)
}

func assertError(rc *RethinkConnection, q RethinkQuery) {
	_, err := rc.Run(q)
	e := err.(RuntimeError)
	fmt.Println("err:", e)
}
