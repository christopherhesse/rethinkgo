package rethinkgo

// Based off of RethinkDB's javascript test.js
// https://github.com/rethinkdb/rethinkdb/blob/next/drivers/javascript/rethinkdb/test.js

// TODO: make sure JS functions/exprs are tested

import (
	"encoding/json"
	"fmt"
	. "launchpad.net/gocheck"
	"testing"
)

// Global expressions used in tests
var arr = Expr(1, 2, 3, 4, 5, 6)
var tobj = Expr(Map{"a": 1, "b": 2, "c": 3})
var tab = Table("table1")
var tab2 = Table("table2")
var tbl = Table("table3")
var tbl4 = Table("table4")
var gobj = Expr(List{
	Map{"g1": 1, "g2": 1, "num": 0},
	Map{"g1": 1, "g2": 2, "num": 5},
	Map{"g1": 1, "g2": 2, "num": 10},
	Map{"g1": 2, "g2": 3, "num": 0},
	Map{"g1": 2, "g2": 3, "num": 100},
})
var j1 = Table("joins1")
var j2 = Table("joins2")
var j3 = Table("joins3")
var docs []Map

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) { TestingT(t) }

type RethinkSuite struct{}

func (s *RethinkSuite) SetUpSuite(c *C) {
	SetDebug(true)
	_, err := Connect("localhost:28015", "test")
	c.Assert(err, IsNil)

	resetDatabase(c)
}

func (s *RethinkSuite) TearDownSuite(c *C) {
	LastSession.Close()
}

func resetDatabase(c *C) {
	// Drop the test database, then re-create it with some test data
	DbDrop("test").Run()
	_, err := DbCreate("test").Run()
	c.Assert(err, IsNil)

	_, err = Db("test").TableCreate("table1").Run()
	c.Assert(err, IsNil)

	pair := ExpectPair{tab.Insert(Map{"id": 0, "num": 20}), Map{"inserted": 1, "errors": 0}}
	runSimpleQuery(c, pair)

	var others []Map
	for i := 1; i < 10; i++ {
		others = append(others, Map{"id": i, "num": 20 - i})
	}
	pair = ExpectPair{tab.Insert(others), Map{"inserted": 9, "errors": 0}}
	runSimpleQuery(c, pair)

	_, err = Db("test").TableCreate("table2").Run()
	c.Assert(err, IsNil)

	pair = ExpectPair{tab2.Insert(List{
		Map{"id": 20, "name": "bob"},
		Map{"id": 19, "name": "tom"},
		Map{"id": 18, "name": "joe"},
	}), Map{"inserted": 3, "errors": 0}}
	runSimpleQuery(c, pair)

	// det
	_, err = Db("test").TableCreate("table3").Run()
	c.Assert(err, IsNil)

	docs = []Map{}
	doc_ids := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	for _, doc_id := range doc_ids {
		docs = append(docs, Map{"id": doc_id})
	}

	tbl.Insert(docs).Run()

	_, err = Db("test").TableCreate("table4").Run()
	c.Assert(err, IsNil)

	// joins tables
	s1 := List{
		Map{"id": 0, "name": "bob"},
		Map{"id": 1, "name": "tom"},
		Map{"id": 2, "name": "joe"},
	}
	s2 := List{
		Map{"id": 0, "title": "goof"},
		Map{"id": 2, "title": "lmoe"},
	}
	s3 := List{
		Map{"it": 0, "title": "goof"},
		Map{"it": 2, "title": "lmoe"},
	}

	Db("test").TableCreate("joins1").Run()
	j1.Insert(s1).Run()
	Db("test").TableCreate("joins2").Run()
	j2.Insert(s2).Run()
	spec := TableSpec{Name: "joins3", PrimaryKey: "it"}
	Db("test").TableCreateSpec(spec).Run()
	j3.Insert(s3).Run()
}

var _ = Suite(&RethinkSuite{})

type jsonChecker struct {
	info *CheckerInfo
}

func (j jsonChecker) Info() *CheckerInfo {
	return j.info
}

func (j jsonChecker) Check(params []interface{}, names []string) (result bool, error string) {
	var jsonParams []interface{}
	for _, param := range params {
		jsonParam, err := json.Marshal(param)
		if err != nil {
			return false, err.Error()
		}
		jsonParams = append(jsonParams, jsonParam)
	}
	return DeepEquals.Check(jsonParams, names)
}

// JsonEquals compares two interface{} objects by converting them to JSON and
// seeing if the strings match
var JsonEquals = &jsonChecker{
	&CheckerInfo{Name: "JsonEquals", Params: []string{"obtained", "expected"}},
}

type ExpectPair struct {
	query    Query
	expected interface{}
}

type MatchMap map[string]interface{}

// Used to indicate that we expect an error from the server
type ErrorResponse struct{}

func runSimpleQuery(c *C, pair ExpectPair) {
	var result interface{}
	fmt.Println("query:", pair.query)
	err := pair.query.RunOne(&result)
	fmt.Printf("result: %v %T\n", result, result)
	_, ok := pair.expected.(ErrorResponse)
	if ok {
		c.Assert(err, NotNil)
		return
	} else {
		c.Assert(err, IsNil)
	}

	// when reading in a number into an interface{}, the json library seems to
	// choose float64 as the type to use
	// since c.Assert() compares the types directly, we need to make sure to pass
	// it a float64 if we have a number
	switch v := pair.expected.(type) {
	case int:
		c.Assert(result, Equals, float64(v))
	case Map, List:
		// Even if v is converted with toObject(), the maps don't seem to compare
		// correctly with gocheck, and the gocheck api docs don't mention maps, so
		// just convert to a []byte with json, then compare the bytes
		c.Assert(result, JsonEquals, pair.expected)
	case MatchMap:
		// In some cases we want to match against a map, but only against those keys
		// that appear in the map, not against all keys in the result, the MatchMap
		// type does this.
		resultMap := result.(map[string]interface{})
		filteredResult := map[string]interface{}{}
		for key, _ := range v {
			filteredResult[key] = resultMap[key]
		}
		c.Assert(filteredResult, JsonEquals, pair.expected)
	default:
		c.Assert(result, Equals, pair.expected)
	}
}

func runStreamQuery(c *C, pair ExpectPair) {
	var result []interface{}
	fmt.Println("query:", pair.query)
	err := pair.query.RunCollect(&result)
	fmt.Printf("result: %v %T\n", result, result)
	c.Assert(err, IsNil)
	c.Assert(result, JsonEquals, pair.expected)
}

var testSimpleGroups = map[string][]ExpectPair{
	"basic": {
		{Expr(1), 1},
		{Expr(true), true},
		{Expr("bob"), "bob"},
		{Expr(nil), nil},
	},
	"arith": {
		{Expr(1).Add(2), 3},
		{Expr(1).Sub(2), -1},
		{Expr(5).Mul(8), 40},
		{Expr(8).Div(2), 4},
		{Expr(7).Mod(2), 1},
	},
	"compare": {
		{Expr(1).Eq(1), true},
		{Expr(1).Eq(2), false},
		{Expr(1).Lt(2), true},
		{Expr(8).Lt(-4), false},
		{Expr(8).Le(8), true},
		{Expr(8).Gt(7), true},
		{Expr(8).Gt(8), false},
		{Expr(8).Ge(8), true},
	},
	"bool": {
		{Expr(true).Not(), false},
		{Expr(true).And(true), true},
		{Expr(true).And(false), false},
		{Expr(true).Or(false), true},
		// DeMorgan's
		{Expr(true).And(false).Eq(Expr(true).Not().Or(Expr(false).Not()).Not()), true},
	},
	"slices": {
		{arr.Nth(0), 1},
		{arr.Count(), 6},
		{arr.Limit(5).Count(), 5},
		{arr.Skip(4).Count(), 2},
		{arr.Skip(4).Nth(0), 5},
		{arr.Slice(1, 4).Count(), 3},
		{arr.Nth(2), 3},
	},
	"append": {
		{arr.Append(7).Nth(6), 7},
	},
	"merge": {
		{Expr(Map{"a": 1}).Merge(Map{"b": 2}), Map{"a": 1, "b": 2}},
	},
	"if": {
		{Branch(true, 1, 2), 1},
		{Branch(false, 1, 2), 2},
		{Branch(Expr(2).Mul(8).Ge(Expr(30).Div(2)), Expr(8).Div(2), Expr(9).Div(3)), 4},
	},
	"let": {
		{Let(Map{"a": 1}, LetVar("a")), 1},
		{Let(Map{"a": 1, "b": 2}, LetVar("a").Add(LetVar("b"))), 3},
	},
	"distinct": {
		{Expr(1, 1, 2, 3, 3, 3, 3).Distinct(), List{1, 2, 3}},
	},
	"map": {
		{arr.Map(func(a Expression) Expression {
			return a.Add(1)
		}).Nth(2),
			4,
		},
	},
	"reduce": {
		{arr.Reduce(0, func(a, b Expression) Expression {
			return a.Add(b)
		}),
			21,
		},
	},
	"filter": {
		{arr.Filter(func(val Expression) Expression {
			return val.Lt(3)
		}).Count(),
			2,
		},
	},
	"contains": {
		{tobj.Contains("a"), true},
		{tobj.Contains("d"), false},
		{tobj.Contains("a", "c"), true},
		{tobj.Contains("a", "d"), false},
	},
	"getattr": {
		{tobj.Attr("a"), 1},
		{tobj.Attr("b"), 2},
		{tobj.Attr("c"), 3},
	},
	"pickattrs": {
		{tobj.Pick("a"), Map{"a": 1}},
		{tobj.Pick("a", "b"), Map{"a": 1, "b": 2}},
	},
	"unpick": {
		{tobj.Unpick("a"), Map{"b": 2, "c": 3}},
		{tobj.Unpick("a", "b"), Map{"c": 3}},
	},
	"r": {
		{Let(Map{"a": Map{"b": 1}}, LetVar("a").Attr("b")), 1},
	},
	"orderby": {
		{tab.OrderBy("num").Nth(2), Map{"id": 7, "num": 13}},
		{tab.OrderBy("num").Nth(2).Pick("num"), Map{"num": 13}},
		{tab.OrderBy(Asc("num")).Nth(2), Map{"id": 7, "num": 13}},
		{tab.OrderBy(Asc("num")).Nth(2).Pick("num"), Map{"num": 13}},
		{tab.OrderBy(Desc("num")).Nth(2), Map{"id": 2, "num": 18}},
		{tab.OrderBy(Desc("num")).Nth(2).Pick("num"), Map{"num": 18}},
	},
	"pluck": {
		{tab.OrderBy("num").Pluck("num").Nth(0), Map{"num": 11}},
	},
	"without": {
		{tab.OrderBy("num").Without("num").Nth(0), Map{"id": 9}},
	},
	"union": {
		{Expr(1, 2, 3).Union(List{4, 5, 6}), List{1, 2, 3, 4, 5, 6}},
		{tab.Union(tab).Count().Eq(tab.Count().Mul(2)), true},
	},
	"tablefilter": {
		{tab.Filter(func(row Expression) Expression {
			return row.Attr("num").Gt(16)
		}).Count(),
			4,
		},
		{tab.Filter(Row.Attr("num").Gt(16)).Count(), 4},
		{tab.Filter(Map{"num": 16}).Nth(0), Map{"id": 4, "num": 16}},
		{tab.Filter(Map{"num": Expr(20).Sub(Row.Attr("id"))}).Count(), 10},
	},
	"tablemap": {
		{tab.OrderBy("num").Map(Row.Attr("num")).Nth(2), 13},
	},
	"tablereduce": {
		{tab.Map(Row.Attr("num")).Reduce(0, func(a, b Expression) Expression { return b.Add(a) }), 155},
	},
	"tablechain": {
		{tab.Filter(func(row Expression) Expression {
			return Row.Attr("num").Gt(16)
		}).Count(),
			4,
		},

		{tab.Map(func(row Expression) Expression {
			return Row.Attr("num").Add(2)
		}).Filter(func(val Expression) Expression {
			return val.Gt(16)
		}).Count(),
			6,
		},

		{tab.Filter(func(row Expression) Expression {
			return Row.Attr("num").Gt(16)
		}).Map(func(row Expression) Expression {
			return row.Attr("num").Mul(4)
		}).Reduce(0, func(acc, val Expression) Expression {
			return acc.Add(val)
		}),
			296,
		},
	},
	"between": {
		{tab.BetweenIds(2, 3).Count(), 2},
		{tab.Between("id", 2, 3).OrderBy("id").Nth(0), Map{"id": 2, "num": 18}},
	},
	"groupedmapreduce": {
		{tab.GroupedMapReduce(
			func(row Expression) Expression {
				return Branch(row.Attr("id").Lt(5), 0, 1)
			},
			func(row Expression) Expression {
				return row.Attr("num")
			},
			0,
			func(acc, num Expression) Expression {
				return acc.Add(num)
			},
		),
			List{
				Map{"group": 0, "reduction": 90},
				Map{"group": 1, "reduction": 65},
			},
		},
	},
	"groupby": {
		{gobj.GroupBy("g1", Avg("num")),
			List{
				Map{"group": 1, "reduction": 5},
				Map{"group": 2, "reduction": 50},
			},
		},
		{gobj.GroupBy("g1", Count()),
			List{
				Map{"group": 1, "reduction": 3},
				Map{"group": 2, "reduction": 2},
			},
		},
		{gobj.GroupBy("g1", Sum("num")),
			List{
				Map{"group": 1, "reduction": 15},
				Map{"group": 2, "reduction": 100},
			},
		},
		// TODO: groupby with multiple keys?
		// {gobj.GroupBy("g1", "g2", Avg("num")),
		// 	List{
		// 		Map{"group": List{1, 1}, "reduction": 0},
		// 		Map{"group": List{1, 2}, "reduction": 7.5},
		// 		Map{"group": List{2, 3}, "reduction": 50},
		// 	},
		// },
	},
	"concatmap": {
		{tab.ConcatMap(List{1, 2}).Count(), 20},
	},
	"update": {
		{tab.Filter(func(row Expression) Expression {
			return row.Attr("id").Ge(5)
		}).Update(func(a Expression) Expression {
			return a.Merge(Map{"updated": true})
		}),
			Map{
				"errors":  0,
				"skipped": 0,
				"updated": 5,
			},
		},
		{tab.Filter(func(row Expression) Expression {
			return row.Attr("id").Lt(5)
		}).Update(func(a Expression) Expression {
			return a.Merge(Map{"updated": true})
		}),
			Map{
				"errors":  0,
				"skipped": 0,
				"updated": 5,
			},
		},
		{tab.Filter(func(row Expression) Expression {
			return row.Attr("updated").Eq(true)
		}).Count(), 10},
	},
	"pointupdate": {
		{tab.GetById(0).Update(func(row Expression) Expression {
			return row.Merge(Map{"pointupdated": true})
		}),
			Map{
				"errors":  0,
				"skipped": 0,
				"updated": 1,
			},
		},
		{tab.GetById(0).Attr("pointupdated"), true},
	},
	"replace": {
		{tab.Replace(func(row Expression) Expression {
			return row.Pick("id").Merge(Map{"mutated": true})
		}),
			Map{
				"deleted":  0,
				"errors":   0,
				"inserted": 0,
				"modified": 10,
			},
		},
		{tab.Filter(func(row Expression) Expression {
			return row.Attr("mutated").Eq(true)
		}).Count(),
			10,
		},
	},
	"pointreplace": {
		{tab.GetById(0).Replace(func(row Expression) Expression {
			return row.Pick("id").Merge(Map{"pointmutated": true})
		}),
			Map{
				"deleted":  0,
				"errors":   0,
				"inserted": 0,
				"modified": 1,
			},
		},
		{tab.GetById(0).Attr("pointmutated"), true},
	},
	"det": {
		{tbl.Update(func(row Expression) interface{} {
			return Map{"count": JS(`0`)}
		}),
			MatchMap{"errors": 10},
		},
		{tbl.Update(func(row Expression) interface{} {
			return Map{"count": 0}
		}),
			MatchMap{"updated": 10},
		},
		{tbl.Replace(func(row Expression) interface{} {
			return tbl.GetById(row.Attr("id"))
		}),
			MatchMap{"errors": 10},
		},
		{tbl.Replace(func(row Expression) interface{} {
			return row
		}),
			MatchMap{},
		},
		{tbl.Update(Map{"count": tbl.Map(func(x Expression) interface{} {
			return x.Attr("count")
		}).Reduce(0, func(a, b Expression) Expression {
			return a.Add(b)
		})}),
			MatchMap{"errors": 10},
		},
		{tbl.Update(Map{"count": Expr(docs).Map(func(x Expression) interface{} {
			return x.Attr("id")
		}).Reduce(0, func(a, b Expression) Expression {
			return a.Add(b)
		})}),
			MatchMap{"updated": 10},
		},
	},
	"nonatomic": {
		{tbl.Update(func(row Expression) interface{} {
			return Map{"count": 0}
		}),
			MatchMap{"updated": 10},
		},
		{tbl.Update(func(row Expression) interface{} {
			return Map{"x": JS(`1`)}
		}),
			MatchMap{"errors": 10},
		},
		{tbl.Update(func(row Expression) interface{} {
			return Map{"x": JS(`1`)}
		}).Atomic(false),
			MatchMap{"updated": 10},
		},
		{tbl.Map(func(row Expression) interface{} {
			return row.Attr("x")
		}).Reduce(0, func(a, b Expression) Expression {
			return a.Add(b)
		}),
			10,
		},
		{tbl.GetById(0).Update(func(row Expression) interface{} {
			return Map{"x": JS(`1`)}
		}),
			ErrorResponse{},
		},
		{tbl.GetById(0).Update(func(row Expression) interface{} {
			return Map{"x": JS(`2`)}
		}).Atomic(false),
			MatchMap{"updated": 1},
		},
		{tbl.Map(func(a Expression) Expression {
			return a.Attr("x")
		}).Reduce(0, func(a, b Expression) Expression {
			return a.Add(b)
		}),
			11,
		},
		{tbl.Update(func(row Expression) interface{} {
			return Map{"x": JS(`x`)}
		}),
			MatchMap{"errors": 10},
		},
		{tbl.Update(func(row Expression) interface{} {
			return Map{"x": JS(`x`)}
		}).Atomic(false),
			MatchMap{"errors": 10},
		},
		{tbl.Map(func(a Expression) Expression {
			return a.Attr("x")
		}).Reduce(0, func(a, b Expression) Expression {
			return a.Add(b)
		}),
			11,
		},
		{tbl.GetById(0).Update(func(row Expression) interface{} {
			return Map{"x": JS(`x`)}
		}),
			ErrorResponse{},
		},
		{tbl.GetById(0).Update(func(row Expression) interface{} {
			return Map{"x": JS(`x`)}
		}).Atomic(false),
			ErrorResponse{},
		},
		{tbl.Map(func(a Expression) Expression {
			return a.Attr("x")
		}).Reduce(0, func(a, b Expression) Expression {
			return a.Add(b)
		}),
			11,
		},
		{tbl.Update(func(row Expression) interface{} {
			return Branch(JS(`true`), nil, Map{"x": 0.1})
		}),
			MatchMap{"errors": 10},
		},
		{tbl.Update(func(row Expression) interface{} {
			return Branch(JS(`true`), nil, Map{"x": 0.1})
		}).Atomic(false),
			MatchMap{"skipped": 10},
		},
		{tbl.Map(func(a Expression) Expression {
			return a.Attr("x")
		}).Reduce(0, func(a, b Expression) Expression {
			return a.Add(b)
		}),
			11,
		},
		{tbl.GetById(0).Replace(func(row Expression) interface{} {
			return Branch(JS(`true`), row, nil)
		}),
			ErrorResponse{},
		},
		{tbl.GetById(0).Replace(func(row Expression) interface{} {
			return Branch(JS(`true`), row, nil)
		}).Atomic(false),
			MatchMap{"modified": 1},
		},
		{tbl.Map(func(a Expression) Expression {
			return a.Attr("x")
		}).Reduce(0, func(a, b Expression) Expression {
			return a.Add(b)
		}),
			11,
		},
		{tbl.Replace(
			Fn("rowA", Branch(
				JS("rowA.id == 1"),
				LetVar("rowA").Merge(Map{"x": 2}),
				LetVar("rowA"),
			))),
			MatchMap{"errors": 10},
		},
		{tbl.Replace(
			Fn("rowA", Branch(
				JS("rowA.id == 1"),
				LetVar("rowA").Merge(Map{"x": 2}),
				LetVar("rowA"),
			))).Atomic(false),
			MatchMap{"modified": 10},
		},
		{tbl.Map(func(a Expression) Expression {
			return a.Attr("x")
		}).Reduce(0, func(a, b Expression) Expression {
			return a.Add(b)
		}),
			12,
		},
		{tbl.GetById(0).Replace(func(row Expression) interface{} {
			return Branch(JS(`x`), row, nil)
		}),
			ErrorResponse{},
		},
		{tbl.GetById(0).Replace(func(row Expression) interface{} {
			return Branch(JS(`x`), row, nil)
		}).Atomic(false),
			ErrorResponse{},
		},
		{tbl.Map(func(a Expression) Expression {
			return a.Attr("x")
		}).Reduce(0, func(a, b Expression) Expression {
			return a.Add(b)
		}),
			12,
		},
		{tbl.GetById(0).Replace(func(row Expression) interface{} {
			return Branch(JS(`true`), nil, row)
		}),
			ErrorResponse{},
		},
		{tbl.GetById(0).Replace(func(row Expression) interface{} {
			return Branch(JS(`true`), nil, row)
		}).Atomic(false),
			MatchMap{"deleted": 1},
		},
		{tbl.Map(func(a Expression) Expression {
			return a.Attr("x")
		}).Reduce(0, func(a, b Expression) Expression {
			return a.Add(b)
		}),
			10,
		},
		{tbl.Replace(
			Fn("rowA", Branch(
				JS("rowA.id < 3"),
				nil,
				LetVar("rowA"),
			))),
			MatchMap{"errors": 9},
		},
		{tbl.Replace(
			Fn("rowA", Branch(
				JS("rowA.id < 3"),
				nil,
				LetVar("rowA"),
			))).Atomic(false),
			MatchMap{"deleted": 2},
		},
		{tbl.Map(func(a Expression) Expression {
			return a.Attr("x")
		}).Reduce(0, func(a, b Expression) Expression {
			return a.Add(b)
		}),
			7,
		},
		{tbl.GetById(0).Replace(
			Map{
				"id":    0,
				"count": tbl.GetById(3).Attr("count"),
				"x":     tbl.GetById(3).Attr("x"),
			}),
			ErrorResponse{},
		},
		{tbl.GetById(0).Replace(
			Map{
				"id":    0,
				"count": tbl.GetById(3).Attr("count"),
				"x":     tbl.GetById(3).Attr("x"),
			}).Atomic(false),
			MatchMap{"inserted": 1},
		},
		{tbl.GetById(1).Replace(
			tbl.GetById(3).Merge(Map{"id": 1}),
		),
			ErrorResponse{},
		},
		{tbl.GetById(1).Replace(
			tbl.GetById(3).Merge(Map{"id": 1}),
		).Atomic(false),
			MatchMap{"inserted": 1},
		},
		{tbl.GetById(2).Replace(
			tbl.GetById(1).Merge(Map{"id": 2}),
		).Atomic(false),
			MatchMap{"inserted": 1},
		},
		{tbl.Map(func(a Expression) Expression {
			return a.Attr("x")
		}).Reduce(0, func(a, b Expression) Expression {
			return a.Add(b)
		}),
			10,
		},
	},
	"delete": {
		{tab.GetById(0).Delete(),
			MatchMap{"deleted": 1},
		},
		{tab.Count(),
			9,
		},
		{tab.Delete(),
			MatchMap{"deleted": 9},
		},
		{tab.Count(),
			0,
		},
	},
	"foreach": {
		{Expr(1, 2, 3).ForEach(func(a Expression) Query {
			return tbl4.Insert(Map{"id": a, "fe": true})
		}),
			MatchMap{"inserted": 3},
		},
	},
}

// function testForEach1() {
//     r([1,2,3]).forEach(function(a) {return tab.insert({id:a, fe:true})}).run(objeq({
//         inserted:3
//     }));
// }

// function testForEach2() {
//     tab.forEach(function(a) {return tab.get(a('id')).update(r({fe:true}))}).run(objeq({
//         updated:3
//     }));
// }

// function testForEach3() {
//     wait();
//     tab.run(function(row) {
//         if (row === undefined) {
//             done();
//             return false;
//         } else {
//             assertEquals(row['fe'], true);
//             return true;
//         }
//     });
// }

// function testClose() {
//     tab.del().run(function() {
//         conn.close();
//     });
// }

// def test_unicode(self):
//     self.clear_table()

//     doc0 = {u"id": 100, u"text": u"グルメ"}
//     doc1 = {"id": 100, u"text": u"グルメ"}

//     doc2 = {u"id": 100, u"text": u"abc"}
//     doc3 = {"id": 100, u"text": u"abc"}
//     doc4 = {u"id": 100, "text": u"abc"}
//     doc5 = {"id": 100, "text": u"abc"}

//     self.do_insert(doc0, True)
//     self.expect(self.table.get(100), doc0)
//     self.expect(self.table.get(100), doc1)
//     self.do_insert(doc1, True)
//     self.expect(self.table.get(100), doc0)
//     self.expect(self.table.get(100), doc1)

//     self.do_insert(doc2, True)
//     self.expect(self.table.get(100), doc2)
//     self.expect(self.table.get(100), doc3)
//     self.expect(self.table.get(100), doc4)
//     self.expect(self.table.get(100), doc5)

//     self.do_insert(doc3, True)
//     self.expect(self.table.get(100), doc2)
//     self.expect(self.table.get(100), doc3)
//     self.expect(self.table.get(100), doc4)
//     self.expect(self.table.get(100), doc5)

//     self.do_insert(doc4, True)
//     self.expect(self.table.get(100), doc2)
//     self.expect(self.table.get(100), doc3)
//     self.expect(self.table.get(100), doc4)
//     self.expect(self.table.get(100), doc5)

//     self.do_insert(doc5, True)
//     self.expect(self.table.get(100), doc2)
//     self.expect(self.table.get(100), doc3)
//     self.expect(self.table.get(100), doc4)
//     self.expect(self.table.get(100), doc5)

var testStreamGroups = map[string][]ExpectPair{
	"join": {
		{j1.InnerJoin(j2, func(one, two Expression) Expression {
			return one.Attr("id").Eq(two.Attr("id"))
		}).Zip().OrderBy("id"),
			List{
				Map{"id": 0, "name": "bob", "title": "goof"},
				Map{"id": 2, "name": "joe", "title": "lmoe"},
			},
		},
		{j1.OuterJoin(j2, func(one, two Expression) Expression {
			return one.Attr("id").Eq(two.Attr("id"))
		}).Zip().OrderBy("id"),
			List{
				Map{"id": 0, "name": "bob", "title": "goof"},
				Map{"id": 1, "name": "tom"},
				Map{"id": 2, "name": "joe", "title": "lmoe"},
			},
		},
		{j1.EqJoin("id", j2, "id").Zip().OrderBy("id"),
			List{
				Map{"id": 0, "name": "bob", "title": "goof"},
				Map{"id": 2, "name": "joe", "title": "lmoe"},
			},
		},
		{j1.EqJoin("id", j3, "it").Zip().OrderBy("id"),
			List{
				Map{"id": 0, "it": 0, "name": "bob", "title": "goof"},
				Map{"id": 2, "it": 2, "name": "joe", "title": "lmoe"},
			},
		},
	},
}

func (s *RethinkSuite) TestGroups(c *C) {
	for group, pairs := range testSimpleGroups {
		if group != "foreach" {
			continue
		}
		for index, pair := range pairs {
			fmt.Println("group:", group, index)
			runSimpleQuery(c, pair)
		}
		resetDatabase(c)
	}

	// for group, pairs := range testStreamGroups {
	// 	for index, pair := range pairs {
	// 		fmt.Println("group:", group, index)
	// 		runStreamQuery(c, pair)
	// 	}
	// 	resetDatabase(c)
	// }
}

func (s *RethinkSuite) TestGet(c *C) {
	for i := 0; i < 10; i++ {
		pair := ExpectPair{tab.GetById(i), Map{"id": i, "num": 20 - i}}
		runSimpleQuery(c, pair)
	}
}

func (s *RethinkSuite) TestOrderBy(c *C) {
	var results1 []Map
	var results2 []Map

	tab.OrderBy("num").RunCollect(&results1)
	tab.OrderBy(Asc("num")).RunCollect(&results2)

	c.Assert(results1, JsonEquals, results2)
}

func (s *RethinkSuite) TestDropTable(c *C) {
	_, err := Db("test").TableCreate("tablex").Run()
	c.Assert(err, IsNil)
	_, err = Db("test").TableDrop("tablex").Run()
	c.Assert(err, IsNil)
}
