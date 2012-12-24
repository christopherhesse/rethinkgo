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

	queries := []Query{
		DbCreate("testdb"),
		DbList(),
		DbDrop("testdb"),
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
		Table("employees").Update(Map{"awesomeness": JS(`row.awesomeness + 1`)}).Atomic(false),
		// Table("employees").Update(Map{"awesomeness": Row.Attr("awesomeness").Add(1)}),

		// Db("test").Table("marvel").GetById("e62a977a-5f03-4f86-95f6-1fc59d10459d").Update(map[string]interface{}{"age": 29}),
		// Db("test").Table("marvel").GetById("e62a977a-5f03-4f86-95f6-1fc59d10459d").Replace(map[string]interface{}{"id": "e62a977a-5f03-4f86-95f6-1fc59d10459d", "name": "Iron Man", "age": 99, "strength": 8}),
		// Db("test").Table("marvel").Filter(map[string]interface{}{"name": "Darkhawk"}).Replace(map[string]interface{}{"id": "f4faf171-9947-4108-9ae2-3f77cd53d012", "name": "Darkhawk", "age": 999, "strength": 9}),
		// // Db("test").Table("dc").GetById("aedfebe7-a60a-4b73-8ecd-680dd4b1ac23").Delete(),
		// // Db("test").Table("dc").Filter(map[string]interface{}{"name": "Unknown"}).Delete(),
		// Db("test").Table("marvel2").Delete(),
		// Db("test").Table("marvel").Map(Row.Attr("strength").Mul(2)),
		// Db("test").Table("marvel").ForEach(func(row Expression) Query {
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

	// spec := TableSpec{Name: "employees", PrimaryKey: "userid"}
	// rows, err := Db("company").TableCreateSpec(spec).Run()
	// TableCreate("employees").SetPrimaryKey("userid")

	// rows, err := Db("company").TableList().Run()
	// rows, err := DbList().Run()
	// var databases []string
	// err = rows.Collect(&databases)
	// fmt.Println("response:", rows, err)

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

func assertError(s *Session, q Query) {
	_, err := s.Run(q)
	e := err.(Error)
	fmt.Println("err:", e)
}

// // Copyright 2010-2012 RethinkDB, all rights reserved.
// // Assumes that rethinkdb is loaded already in whichever environment we happen to be running in

// function print(msg) {
//     console.log(msg);
// }

// var r = rethinkdb;

// var conn;
// function testConnect() {
//     wait();
//     conn = r.connect({host:HOST, port:PORT}, function() {
//         r.db('test').tableList().run(function(table) {
//             if (table) {
//                 r.db('test').tableDrop(table).run();
//                 return true;
//             } else {
//                 r.db('test').tableCreate('test').run(function() {
//                     done();
//                 });
//                 return false;
//             }
//         });
//     }, function(e) {
//         fail(e);
//     });
// }

// function testBasic() {
//     r(1).run(aeq(1));
//     r(true).run(aeq(true));
//     r('bob').run(aeq('bob'));
// }

// function testArith() {
//     r(1).add(2).run(aeq(3));
//     r(1).sub(2).run(aeq(-1));
//     r(5).mul(8).run(aeq(40));
//     r(8).div(2).run(aeq(4));
//     r(7).mod(2).run(aeq(1));

//     r(12).div(3).add(2).mul(3).run(aeq(18));
// }

// function testCompare() {
//     r(1).eq(1).run(aeq(true));
//     r(1).eq(2).run(aeq(false));
//     r(1).lt(2).run(aeq(true));
//     r(8).lt(-4).run(aeq(false));
//     r(8).le(8).run(aeq(true));
//     r(8).gt(7).run(aeq(true));
//     r(8).gt(8).run(aeq(false));
//     r(8).ge(8).run(aeq(true));
// }

// function testBool() {
//     r(true).not().run(aeq(false));
//     r(true).and(true).run(aeq(true));
//     r(true).and(false).run(aeq(false));
//     r(true).or(false).run(aeq(true));

//     // Test DeMorgan's rule!
//     r(true).and(false).eq(r(true).not().or(r(false).not()).not()).run(aeq(true));
// }

// var arr = r([1,2,3,4,5,6]);
// function testSlices() {
//     arr.nth(0).run(aeq(1));
//     arr.count().run(aeq(6));
//     arr.limit(5).count().run(aeq(5));
//     arr.skip(4).count().run(aeq(2));
//     arr.skip(4).nth(0).run(aeq(5));
//     arr.slice(1,4).count().run(aeq(3));
//     arr.nth(2).run(aeq(3));
// }

// function testAppend() {
//     arr.append(7).nth(6).run(aeq(7));
// }

// function testMerge() {
//     r({a:1}).merge({b:2}).run(objeq({a:1,b:2}));
// }

// function testIf() {
//     r.branch(r(true), r(1), r(2)).run(aeq(1));
//     r.branch(r(false), r(1), r(2)).run(aeq(2));
//     r.branch(r(2).mul(8).ge(r(30).div(2)),
//         r(8).div(2),
//         r(9).div(3)).run(aeq(4));
// }

// function testLet() {
//     r.let({a:r(1)}, r.letVar('a')).run(aeq(1));
//     r.let({a:r(1), b:r(2)}, r.letVar('a').add(r.letVar('b'))).run(aeq(3));
// }

// function testDistinct() {
//     var cursor = r([1,1,2,3,3,3,3]).distinct().run(objeq(1,2,3));
// }

// function testMap() {
//     arr.map(function(a) {return a.add(1)}).nth(2).run(aeq(4));
// }

// function testReduce() {
//     arr.reduce(0, function(a, b) {return a.add(b)}).run(aeq(21));
// }

// function testFilter() {
//     arr.filter(function(val) {
//         return val.lt(3);
//     }).count().run(objeq(2));
// }

// var tobj = r({a:1,b:2,c:3});
// function testContains() {
//     tobj.contains('a').run(aeq(true));
//     tobj.contains('d').run(aeq(false));
//     tobj.contains('a', 'b').run(aeq(true));
//     tobj.contains('a', 'd').run(aeq(false));
// }

// function testGetAttr() {
//     tobj.getAttr('a').run(aeq(1));
//     tobj.getAttr('b').run(aeq(2));
//     tobj.getAttr('c').run(aeq(3));

//     r.let({a:tobj},
//         r.branch(r.letVar('a').contains('b'),
//             r.letVar('a')('b'),
//             r.error("No attribute b")
//         )
//     ).run(aeq(2));
// }

// function testPickAttrs() {
//     tobj.pick('a').run(objeq({a:1}));
//     tobj.pick('a', 'b').run(objeq({a:1,b:2}));
// }

// function testUnpick() {
//     tobj.unpick('a').run(objeq({b:2,c:3}));
//     tobj.unpick('a','b').run(objeq({c:3}));
// }

// function testR() {
//     r.let({a:r({b:1})}, r.letVar('a')('b')).run(aeq(1));
// }

// var tab = r.table('test');
// function testInsert() {
//     tab.insert({id:0, num:20}).run(objeq({inserted:1}));

//     var others = [];
//     for (var i = 1; i < 10; i++) {
//         others.push({id:i, num:20-i});
//     }

//     tab.insert(others).run(objeq({inserted:9}));
// }

// function testGet() {
//     for (var i = 0; i < 10; i++) {
//         tab.get(i).run(objeq({id:i,num:20-i}));
//     }
// }

// function testOrderby() {
//     tab.orderBy('num').nth(2).run(objeq({id:7,num:13}));
//     tab.orderBy('num').nth(2).pick('num').run(objeq({num:13}));

//     tab.orderBy(['num', true]).nth(2).run(objeq({id:7,num:13}));
//     tab.orderBy(['num', true]).nth(2).pick('num').run(objeq({num:13}));

//     tab.orderBy(['num', false]).nth(2).run(objeq({id:2,num:18}));
//     tab.orderBy(['num', false]).nth(2).pick('num').run(objeq({num:18}));

//     tab.orderBy(r.asc('num')).nth(2).run(objeq({id:7,num:13}));
//     tab.orderBy(r.asc('num')).nth(2).pick('num').run(objeq({num:13}));

//     tab.orderBy(r.desc('num')).nth(2).run(objeq({id:2,num:18}));
//     tab.orderBy(r.desc('num')).nth(2).pick('num').run(objeq({num:18}));

//     var cur1 = tab.orderBy(r.asc('num')).run();
//     var cur2 = tab.orderBy('num').run();

//     function objEqual(one, two) {
//         for (var key in one) {
//             if (one.hasOwnProperty(key)) {
//                 if (two.hasOwnProperty(key)) {
//                     var propOne = one[key];
//                     var propTwo = two[key];
//                     if (typeof propOne === 'object') {
//                         objEqual(propOne, propTwo);
//                     } else {
//                         assertEquals(propOne, propTwo);
//                     }
//                 } else {
//                     fail('result missing property '+key);
//                 }
//             }
//         }
//     }

//     wait();
//     cur1.collect(function(one) {
//         cur2.collect(function(two) {
//             for(var i = 0; i < one.length; i++) {
//                 objEqual(one[i],two[i]);
//             }
//             done();
//         });
//     });
// }

// function testPluck() {
//     tab.orderBy('num').pluck('num').nth(0).run(objeq({num:11}));
// }

// function testWithout() {
//     tab.orderBy('num').without('num').nth(0).run(objeq({id:9}));
// }

// function testUnion() {
//     r([1,2,3]).union([4,5,6]).run(objeq(1,2,3,4,5,6));
//     tab.union(tab).count().eq(tab.count().mul(2)).run(aeq(true));
// }

// function testTabFilter() {
//     tab.filter(function(row) {
//         return row('num').gt(16);
//     }).count().run(aeq(4));

//     tab.filter(function(row) {return row('num').gt(16)}).count().run(aeq(4));

//     tab.filter(r.row('num').gt(16)).count().run(aeq(4));

//     tab.filter({num:16}).nth(0).run(objeq({id:4,num:16}));

//     tab.filter({num:r(20).sub(r.row('id'))}).count().run(aeq(10));
// }

// function testTabMap() {
//     tab.orderBy('num').map(r.row('num')).nth(2).run(aeq(13));
// }

// function testTabReduce() {
//     tab.map(r.row('num')).reduce(r(0), function(a,b) {return b.add(a)}).run(aeq(155));
//     tab.map(r.row('num')).reduce(r(0), function(a,b) {return b.add(a)}).run(aeq(155));
// }

// function testJS() {
//     tab.filter(function(row) {
//         return row('num').gt(16);
//     }).count().run(aeq(4));

//     tab.map(function(row) {
//         return row('num').add(2);
//     }).filter(function (val) {
//         return val.gt(16);
//     }).count().run(aeq(6));

//     tab.filter(function(row) {
//         return row('num').gt(16);
//     }).map(function(row) {
//         return row('num').mul(4);
//     }).reduce(0, function(acc, val) {
//         return acc.add(val);
//     }).run(aeq(296));
// }

// function testBetween() {
//     tab.between(2,3).count().run(aeq(2));
//     tab.between(2,3).orderBy('id').nth(0).run(objeq({
//         id:2,
//         num:18
//     }));
// }

// function testGroupedMapReduce() {
//     tab.groupedMapReduce(function(row) {
//         return r.branch(row('id').lt(5),
//             r(0),
//             r(1))
//     }, function(row) {
//         return row('num');
//     }, 0, function(acc, num) {
//         return acc.add(num);
//     }).run(objeq(
//         {group:0, reduction:90},
//         {group:1, reduction:65}
//     ));
// }

// function testGroupBy() {
//     var s = r([
//         {g1: 1, g2: 1, num: 0},
//         {g1: 1, g2: 2, num: 5},
//         {g1: 1, g2: 2, num: 10},
//         {g1: 2, g2: 3, num: 0},
//         {g1: 2, g2: 3, num: 100}
//     ]);

//     s.groupBy('g1', r.avg('num')).run(objeq(
//         {group:1, reduction:5},
//         {group:2, reduction:50}
//     ));
//     s.groupBy('g1', r.count).run(objeq(
//         {group:1, reduction:3},
//         {group:2, reduction:2}
//     ));
//     s.groupBy('g1', r.sum('num')).run(objeq(
//         {group:1, reduction:15},
//         {group:2, reduction:100}
//     ));
//     s.groupBy('g1', 'g2', r.avg('num')).run(objeq(
//         {group:[1,1], reduction: 0},
//         {group:[1,2], reduction: 7.5},
//         {group:[2,3], reduction: 50}
//     ));
// }

// function testConcatMap() {
//     tab.concatMap(r([1,2])).count().run(aeq(20));
// }

// function testJoin1() {
//     var s1 = [{id:0, name:'bob'}, {id:1, name:'tom'}, {id:2, name:'joe'}];
//     var s2 = [{id:0, title:'goof'}, {id:2, title:'lmoe'}];
//     var s3 = [{it:0, title:'goof'}, {it:2, title:'lmoe'}];

//     wait();
//     r.db('test').tableCreate('joins1').run(function() {
//         r.table('joins1').insert(s1).run(done);
//     });

//     wait();
//     r.db('test').tableCreate('joins2').run(function() {
//         r.table('joins2').insert(s2).run(done);
//     });

//     wait();
//     r.db('test').tableCreate({tableName:'joins3', primaryKey:'it'}).run(function() {
//         r.table('joins3').insert(s3).run(done);
//     });
// }

// function testJoin2() {
//     var s1 = r.table('joins1');
//     var s2 = r.table('joins2');
//     var s3 = r.table('joins3');

//     s1.innerJoin(s2, function(one, two) {
//         return one('id').eq(two('id'));
//     }).zip().orderBy('id').run(objeq(
//         {id:0, name: 'bob', title: 'goof'},
//         {id:2, name: 'joe', title: 'lmoe'}
//     ));

//     s1.outerJoin(s2, function(one, two) {
//         return one('id').eq(two('id'));
//     }).zip().orderBy('id').run(objeq(
//         {id:0, name: 'bob', title: 'goof'},
//         {id:1, name: 'tom'},
//         {id:2, name: 'joe', title: 'lmoe'}
//     ));

//     s1.eqJoin('id', s2).zip().orderBy('id').run(objeq(
//         {id:0, name: 'bob', title: 'goof'},
//         {id:2, name: 'joe', title: 'lmoe'}
//     ));

//     s1.eqJoin('id', s3, 'it').zip().orderBy('id').run(objeq(
//         {id:0, it:0, name: 'bob', title: 'goof'},
//         {id:2, it:2, name: 'joe', title: 'lmoe'}
//     ));
// }

// var tab2 = r.table('table2');
// function testSetupOtherTable() {
//     wait();
//     r.db('test').tableCreate('table2').run(function() {
//         tab2.insert([
//             {id:20, name:'bob'},
//             {id:19, name:'tom'},
//             {id:18, name:'joe'}
//         ]).run(objeq({inserted:3}));
//         done();
//     });
// }

// function testDropTable() {
//     r.db('test').tableDrop('table-2').run();
// }

// function testUpdate1() {
//     tab.filter(function(row) {
//         return row('id').ge(5);
//     }).update(function(a) {
//         return a.merge({updated:true});
//     }).run(objeq({
//         errors:0,
//         skipped:0,
//         updated:5,
//     }));

//     tab.filter(function(row) {
//         return row('id').lt(5);
//     }).update({updated:true}).run(objeq({
//         errors:0,
//         skipped:0,
//         updated:5,
//     }));
// }

// function testUpdate2() {
//     wait();
//     tab.run(function(row) {
//         if (row === undefined) {
//             done();
//             return false;
//         } else {
//             assertEquals(row['updated'], true);
//             return true;
//         }
//     });
// }

// function testPointUpdate1() {
//     tab.get(0).update(function(a) {
//         return a.merge({pointupdated:true});
//     }).run(objeq({
//         errors:0,
//         skipped:0,
//         updated:1
//     }));
// }

// function testPointUpdate2() {
//     tab.get(0)('pointupdated').run(aeq(true));
// }

// function testReplace1() {
//     tab.replace(function(a) {
//         return a.pick('id').merge({mutated:true});
//     }).run(objeq({
//         deleted:0,
//         errors:0,
//         inserted:0,
//         modified:10
//     }));
// }

// function testReplace2() {
//     wait();
//     tab.run(function(row) {
//         if (row === undefined) {
//             done();
//             return false;
//         } else {
//             assertEquals(row['mutated'], true);
//             return true;
//         }
//     });
// }

// function testPointReplace1() {
//     tab.get(0).replace(function(a) {
//         return a.pick('id').merge({pointmutated:true});
//     }).run(objeq({
//         deleted:0,
//         errors:0,
//         inserted:0,
//         modified:1
//     }));
// }

// function testPointReplace2() {
//     tab.get(0)('pointmutated').run(aeq(true));
// }

// var docs = [0,1,2,3,4,5,6,7,8,9];
// docs = docs.map(function(x) {return {id:x}});
// var tbl = r.table('tbl');

// function testSetupDetYNonAtom() {
//     wait();
//     r.db('test').tableCreate('tbl').run(function() {
//         tbl.insert(docs).run(function() {
//             done();
//         });
//     });
// }

// function testDet() {
//     tbl.update(function(row) {return {count: r.js('0')}}).run(
//         objeq({errors:10, updated:0, skipped:0}));
//     tbl.update(function(row) {return {count: 0}}).run(
//         objeq({errors:0, updated:10, skipped:0}));

//     wait();
//     tbl.replace(function(row) {return tbl.get(row('id'))}).run(function(res) {
//         assertEquals(res.errors, 10);
//         done();
//     });
//     wait();
//     tbl.replace(function(row) {return row}).run(function(possible_err) {
//         assertEquals(possible_err instanceof Error, false);
//         done();
//     });

//     wait();
//     tbl.update({count: tbl.map(function(x) {
//         return x('count')
//     }).reduce(0, function(a,b) {
//         return a.add(b);
//     })}).run(function(res) {
//         assertEquals(res.errors, 10);
//         done();
//     });

//     wait();
//     tbl.update({count: r(docs).map(function(x) {
//         return x('id')
//     }).reduce(0, function(a,b) {
//         return a.add(b);
//     })}).run(function(res) {
//         assertEquals(res.updated, 10);
//         done();
//     });
// }

// var rerr = rethinkdb.errors.RuntimeError;
// function testNonAtomic1() {

//     // Update modify
//     tbl.update(function(row) {return {x: r.js('1')}}).run(attreq('errors', 10));
//     tbl.update(function(row) {return {x: r.js('1')}}, true).run(attreq('updated', 10));
// }

// function testNonAtomic2() {
//     tbl.map(function(row) {return row('x')}).reduce(0, function(a,b) {return a.add(b)}).run(aeq(10));

//     tbl.get(0).update(function(row) {return {x: r.js('1')}}).run(atype(rerr));
//     tbl.get(0).update(function(row) {return {x: r.js('2')}}, true).run(
//         attreq('updated', 1));
// }

// function testNonAtomic3() {
//     tbl.map(function(a){return a('x')}).reduce(0, function(a,b){return a.add(b);}).run(aeq(11));

//     // Update error
//     tbl.update(function(row){return {x:r.js('x')}}).run(attreq('errors', 10));
//     tbl.update(function(row){return {x:r.js('x')}}, true).run(attreq('errors', 10));
//     tbl.map(function(a){return a('x')}).reduce(0, function(a,b){return a.add(b);}).run(aeq(11));
//     tbl.get(0).update(function(row){return {x:r.js('x')}}).run(atype(rerr));
//     tbl.get(0).update(function(row){return {x:r.js('x')}}, true).run(atype(rerr));
// }

// function testNonAtomic4() {
//     tbl.map(function(a){return a('x')}).reduce(0, function(a,b){return a.add(b);}).run(aeq(11));

//     // Update skipped
//     tbl.update(function(row){return r.branch(r.js('true'),
//                                         r(null),
//                                         r({x:0.1}))}).run(attreq('errors',10));
//     tbl.update(function(row){return r.branch(r.js('true'),
//                                         r(null),
//                                         r({x:0.1}))}, true).run(attreq('skipped',10));
// }

// function testNonAtomic5() {
//     tbl.map(function(a){return a('x')}).reduce(0, function(a,b){return a.add(b);}).run(aeq(11));
//     tbl.get(0).update(function(row){return r.branch(r.js('true'),
//                                             r(null),
//                                             r({x:0.1}))}).run(atype(rerr));
//     tbl.get(0).update(function(row){return r.branch(r.js('true'),
//                                             r(null),
//                                             r({x:0.1}))}, true).run(attreq('skipped', 1));
//     tbl.map(function(a){return a('x')}).reduce(0, function(a,b){return a.add(b);}).run(aeq(11));

//     // Replace modify
//     tbl.get(0).replace(function(row){return r.branch(r.js('true'), row, r(null))}).run(
//         atype(rerr));
//     tbl.get(0).replace(function(row){return r.branch(r.js('true'), row, r(null))},
//         true).run(attreq('modified', 1));
//     tbl.map(function(a){return a('x')}).reduce(0, function(a,b){return a.add(b);}).run(aeq(11));
//     tbl.replace(r.fn('rowA', r.branch(r.js('rowA.id == 1'), r.letVar('rowA').merge({x:2}),
//         r.letVar('rowA')))).run(attreq('errors', 10));
//     tbl.replace(r.fn('rowA', r.branch(r.js('rowA.id == 1'), r.letVar('rowA').merge({x:2}),
//         r.letVar('rowA'))), true).run(attreq('modified', 10));
// }

// function testNonAtomic6() {
//     tbl.map(function(a){return a('x')}).reduce(0, function(a,b){return a.add(b);}).run(aeq(12));

//     // Replace error
//     tbl.get(0).replace(function(row){return r.branch(r.js('x'), row, r(null))}).run(
//         atype(rerr));
//     tbl.get(0).replace(function(row){return r.branch(r.js('x'), row, r(null))},
//         true).run(atype(rerr));
//     tbl.map(function(a){return a('x')}).reduce(0, function(a,b){return a.add(b);}).run(aeq(12));

//     // Replace delete
//     tbl.get(0).replace(function(row){return r.branch(r.js('true'), r(null), row)}).run(
//         atype(rerr));
//     tbl.get(0).replace(function(row){return r.branch(r.js('true'), r(null), row)},
//         true).run(attreq('deleted', 1));
// }

// function testNonAtomic7() {
//     tbl.map(function(a){return a('x')}).reduce(0, function(a,b){return a.add(b);}).run(aeq(10));
//     tbl.replace(r.fn('rowA', r.branch(r.js('rowA.id < 3'), r(null),
//         r.letVar('rowA')))).run(attreq('errors', 9));
//     tbl.replace(r.fn('rowA', r.branch(r.js('rowA.id < 3'), r(null),
//         r.letVar('rowA'))), true).run(attreq('deleted', 2));
// }

// function testNonAtomic8() {
//     tbl.map(function(a){return a('x')}).reduce(0, function(a,b){return a.add(b);}).run(aeq(7));

//     // Replace insert
//     tbl.get(0).replace({id:0, count:tbl.get(3)('count'), x:tbl.get(3)('x')}).run(atype(rerr));
//     tbl.get(0).replace({id:0, count:tbl.get(3)('count'), x:tbl.get(3)('x')}, true).run(
//         attreq('inserted', 1));
//     tbl.get(1).replace(tbl.get(3).merge({id:1})).run(atype(rerr));
//     tbl.get(1).replace(tbl.get(3).merge({id:1}), true).run(attreq('inserted', 1));
//     tbl.get(2).replace(tbl.get(1).merge({id:2}), true).run(attreq('inserted', 1));
// }

// function testNonAtomic9() {
//     tbl.map(function(a){return a('x')}).reduce(0, function(a,b){return a.add(b);}).run(aeq(10));
// }

// function testPointDelete1() {
//     tab.get(0).del().run(objeq({
//         deleted:1
//     }));
// }

// function testPointDelete2() {
//     tab.count().run(aeq(9));
// }

// function testDelete1() {
//     tab.count().run(aeq(9));
//     tab.del().run(objeq({deleted:9}));
// }

// function testDelete2() {
//     tab.count().run(aeq(0));
// }

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

// runTests([
//     testConnect,
//     testBasic,
//     testCompare,
//     testArith,
//     testBool,
//     testSlices,
//     testAppend,
//     testMerge,
//     testIf,
//     testLet,
//     testDistinct,
//     testMap,
//     testReduce,
//     testFilter,
//     testContains,
//     testGetAttr,
//     testPickAttrs,
//     testUnpick,
//     testR,
//     testInsert,
//     testGet,
//     testOrderby,
//     testPluck,
//     testWithout,
//     testUnion,
//     testTabFilter,
//     testTabMap,
//     testTabReduce,
//     testJS,
//     testBetween,
//     testGroupedMapReduce,
//     testGroupBy,
//     testConcatMap,
//     testJoin1,
//     testJoin2,
//     testSetupOtherTable,
//     testDropTable,
//     testUpdate1,
//     testUpdate2,
//     testPointUpdate1,
//     testPointUpdate2,
//     testReplace1,
//     testReplace2,
//     testPointReplace1,
//     testPointReplace2,
//     testSetupDetYNonAtom,
//     testDet,
//     testNonAtomic1,
//     testNonAtomic2,
//     testNonAtomic3,
//     testNonAtomic4,
//     testNonAtomic5,
//     testNonAtomic6,
//     testNonAtomic7,
//     testNonAtomic8,
//     testNonAtomic9,
//     testPointDelete1,
//     testPointDelete2,
//     testDelete1,
//     testDelete2,
//     testForEach1,
//     testForEach2,
//     testForEach3,
//     testClose,
// ]);
