package rethinkgo

import (
	"fmt"
	"testing"
)

type Hero struct {
	Name     string `json:"name"`
	Strength int    `json:"strength"`
}

type Employee struct {
	Id        string
	FirstName string
	LastName  string
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
	// assertError(rc, Db("test").TableDrop("testtable"))
	// assertError(rc, Error("test"))

	employees := Db("test").Table("employees")

	gmr := GroupedMapReduce{
		Mapping:   JS(`this.awesomeness`),
		Base:      0,
		Reduction: JS(`acc + row`),
		Finalizer: nil,
	}

	// dcCharacter := Hero{Name: "Bruce Wayne", Strength: 5}

	// TODO: test useoutdated, useoutdated(false), default database on connection, database overriding by connection, two connections

	queries := []RethinkQuery{
		DBCreate("testdb"),
		DBList(),
		DBDrop("testdb"),
		JS("({name:2})").Attr("name"),
		employees.GroupBy("awesomeness", Sum("awesomeness")),
		employees.Map(func(row Expression) interface{} { return 1 }),
		employees.Map(func(row Expression) interface{} { return row.Attr("awesomeness") }),
		employees.Map(Row.Attr("awesomeness")),
		employees.Map(JS(`this.first_name[0] + ' Fucking ' + this.last_name[0]`)),
		employees.GroupBy("awesomeness", gmr),
		Expr(1, 2, 3),
		JS(`[1,2,3]`),
		Expr(1, 2, 3).Add(Expr(4, 5, 6)),
		Expr(3).Div(2),
		employees.Map(Row.Attr("awesomeness")),
		employees.Filter(Map{"first_name": "Marc"}),

		// employees.Map(Branch(Row.Attr("first_name").Eq("Marc"), "is probably marc", "who cares")),

		// Db("test").Table("employees").Map(JS(`return this.joeyness > this.marcness ? "is joey" : "is probably marc"`)),
		// Db("test").TableCreate("testtable"),
		// Db("test").TableList(),
		// Db("test").TableDrop("testtable"),
		// characters.Map(
		// 	func(row Expression) interface{} {
		// 		return row.Attr("awesomeness").Mul(2)
		// 	}),
		// Expr(2).Mul(2),
		// characters.Get("f001af8b-7d11-45a4-a268-a073ad4756ff", "id"),
		// characters.GetById("f001af8b-7d11-45a4-a268-a073ad4756ff"),
		// characters.GetById(JS(`"f001af8b-7d11-45a4-a268-a073ad4756ff"`)),
		// characters.GetById("f001af8b-7d11-45a4-a268-a073ad4756ff").Contains("show"),
		// characters.GetById("f001af8b-7d11-45a4-a268-a073ad4756ff").Pick("show"),
		// Expr(nil),
		// Expr([]int{1, 2, 3}).ArrayToStream().Nth(2),
		// Expr([]int{1, 2, 3}).ArrayToStream().Slice(3, nil),
		// characters.BetweenIds("199bbc8c-d48e-470a-b79b-b853e0881099", "c384aec7-53ff-4c68-ac9e-233317af44f4"),
		// characters.Filter(
		// 	func(row Expression) interface{} {
		// 		return Attr("name").Eq("William Adama")
		// 	}),
		// characters.Filter(map[string]interface{}{"name": "William Adama", "show": "Battlestar Galactica"}),
		// characters.OrderBy("name", Desc("awesomeness")),
		// characters.Map(
		// 	func(row Expression) interface{} {
		// 		return Attr("awesomeness")
		// 	}).Reduce(0,
		// 	func(acc, row Expression) interface{} {
		// 		return acc.Add(row)
		// 	}),
		// characters.GroupedMapReduce(
		// 	func(row Expression) interface{} {
		// 		return row.Attr("awesomeness")
		// 	},
		// 	func(row Expression) interface{} {
		// 		return row.Attr("awesomeness")
		// 	},
		// 	0,
		// 	func(acc, row Expression) interface{} {
		// 		return acc.Add(row)
		// 	},
		// ),
		// characters.UseOutdated(true),
		// characters.Pluck("name", "show"),
		// characters.GroupedMapReduce(
		// 	func(row Expression) Expression {
		// 		return row.Attr("awesomeness")
		// 	},
		// 	func(row Expression) Expression {
		// 		return row.Attr("name")
		// 	},
		// 	"",
		// 	JS(`acc + row`),
		// ),
		// characters.Map(JS(`[row.awesomeness * 2, 3]`)),
		// // Expr(func() interface{} { return map[string]interface{}{"name": 3} }).Attr("name"),
		// Expr([]int{1, 2, 3}).ArrayToStream().Map(JS(`row*2`)),
		// Db("test").Table("marvel").OuterJoin(Db("test").Table("dc"),
		// 	func(marvel, dc Expression) interface{} {
		// 		return marvel.Attr("strength").Eq(dc.Attr("strength"))
		// 	}),
		// Db("test").Table("marvel").EqJoin("id", Db("test").Table("dc"), "id"),
		// Expr([]int{1, 2, 3}).Skip(1).Count(),
		// Expr([]int{1, 2, 2, 3, 2, 2}).ArrayToStream().Distinct(),
		// Db("test").Table("dc").Insert(map[string]interface{}{"name": "Unknown", "strength": 1}),
		// Db("test").Table("dc").Insert(dcCharacter),
		// Db("test").Table("marvel").Update(map[string]interface{}{"age": 31}),
		// Db("test").Table("marvel").GetById("e62a977a-5f03-4f86-95f6-1fc59d10459d").Update(map[string]interface{}{"age": 29}),
		// Db("test").Table("marvel").GetById("e62a977a-5f03-4f86-95f6-1fc59d10459d").Replace(map[string]interface{}{"id": "e62a977a-5f03-4f86-95f6-1fc59d10459d", "name": "Iron Man", "age": 99, "strength": 8}),
		// Db("test").Table("marvel").Filter(map[string]interface{}{"name": "Darkhawk"}).Replace(map[string]interface{}{"id": "f4faf171-9947-4108-9ae2-3f77cd53d012", "name": "Darkhawk", "age": 999, "strength": 9}),
		// // Db("test").Table("dc").GetById("aedfebe7-a60a-4b73-8ecd-680dd4b1ac23").Delete(),
		// // Db("test").Table("dc").Filter(map[string]interface{}{"name": "Unknown"}).Delete(),
		// Db("test").Table("marvel2").Delete(),
		// Db("test").Table("marvel").Map(Row.Attr("strength").Mul(2)),
		// Db("test").Table("marvel").ForEach(func(row Expression) RethinkQuery {
		// 	return Db("test").Table("marvel2").Insert(row)
		// }),
		// Db("test").Table("table_that_doesnt_exist"),
	}

	for _, query := range queries {
		result, err := rc.Run(query)
		fmt.Println("query struct:", query, "result:", result, "err:", err)
		if err != nil {
			panic(err)
		}
	}

	// q := employees.Map(Row.Attr("awesomeness"))
	// result, err := q.Run()
	// fmt.Println("q:", result, err)

	// binds := map[string]interface{}{"joey": Db("test").Table("employees").GetById("Joey")}
	// expr := Row.Attr("awesomeness").Mul(LetVar("joey").Attr("awesomeness"))
	// let := Let(binds, expr)
	// doesnt work: cant do table query
	// body := `var joey = this.table("employees").get("Joey"); return this.awesomeness * joey.awesomeness`
	// fmt.Println(rc.Run(Db("test").Table("employees").Map(JS(body))))

	// rows, err := rc.Run(Db("test").Table("marvel"))
	// fmt.Println("err:", err)
	// heroes := []Hero{}
	// err = rows.Collect(&heroes)
	// fmt.Println("err:", err)
	// fmt.Println("heroes:", heroes)

	// result, err := rc.RunSingle()
	// var response InsertResponse
	// result.Scan(&response)
	// fmt.Println("insert:", response, err)
}

func assertError(s *Session, q RethinkQuery) {
	_, err := s.Run(q)
	e := err.(RethinkError)
	fmt.Println("err:", e)
}
