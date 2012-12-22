package rethinkdb

// Let user create queries as RQL Expression trees, any errors are deferred
// until the query is run, so most all functions take interface{} types.
// interface{} is effectively a void* type that we look at later to determine
// the underlying type and perform any conversions.

// Query is the type returned by any call that terminates a query (for instance,
// .Insert()), and provides .Run() and .RunSingle() methods to run the Query on
// the last created connection.
type Query struct {
	// this is apparently called an embedded interface
	RethinkQuery
}

type expressionKind int

const (
	// These I just made up
	literalKind expressionKind = iota // converted to an Expression
	groupByKind
	useOutdatedKind

	///////////
	// Terms //
	///////////
	// Null
	variableKind
	letKind
	// Call - implicit for all Builtin types
	ifKind
	errorKind
	// Number - stored as JSON
	// String - stored as JSON
	// JSON - implicitly specified (use anything that is not Array or Object)
	// Bool - stored as JSON
	// Array - implicitly specified as type []interface{}
	// Object - implicitly specified as type map[string]interface{}
	getByKeyKind
	tableKind
	javascriptKind
	implicitVariableKind

	//////////////
	// Builtins //
	//////////////
	// these are all subtypes of the CALL term type
	logicalNotKind
	getAttributeKind
	implicitGetAttributeKind
	hasAttributeKind
	// ImplictHasAttribute - appears to be unused
	pickAttributesKind
	// ImplicitPickAttributes - appears to be unused
	mapMergeKind
	arrayAppendKind
	sliceKind
	addKind
	subtractKind
	multiplyKind
	divideKind
	moduloKind
	// Compare - implicit for all compare subtypes below
	filterKind
	mapKind
	concatMapKind
	orderByKind
	distinctKind
	lengthKind
	unionKind
	nthKind
	streamToArrayKind
	arrayToStreamKind
	reduceKind
	groupedMapReduceKind
	logicalOrKind
	logicalAndKind
	rangeKind
	withoutKind
	// ImplicitWithout - appears to be unused
	// Compare sub-types
	equalityKind
	inequalityKind
	greaterThanKind
	greaterThanOrEqualKind
	lessThanKind
	lessThanOrEqualKind
)

// Expression represents an RQL expression, such as .Filter(), which, when
// called on another expression, filters the results of that expression when run
// on the server.  It is used as the argument type of any functions used in RQL.
//
// To create an expression from a native type, or user-defined type, or
// function, use Expr().  In most cases, this is not necessary as conversion to
// an expression will be done automatically.
//
// Example usage:
//
//  r.Table("just_twos").Map(func(row Expression) interface{} { return 1 })
//
// The func() object as well as the '1' constant could each be wrapped in an
// Expr() call but that is done automatically by this library.
type Expression struct {
	kind  expressionKind
	value interface{}
}

// Row supplies access to the current row in any query, even if there's no go
// func with a reference to it.
//
// Example without Row:
//
//  r.Table("employees").Map(func(row Expression) interface{} {
//      return row.Attr("awesomeness")
//  })
//
// Example with Row:
//
//  r.Table("employees").Map(Row.Attr("awesomeness"))
var Row = Expression{kind: implicitVariableKind}

// Expr converts any value to an expression.  Internally it uses the `json`
// module to convert any literals, so any type annotations or methods understood
// by that module can be used. If the value cannot be converted, an error is
// returned at query .Run() time.
//
// Example usage:
//
//  r.Expr(map[string]string{"go": "awesome", "rethinkdb": "awesomer"})
func Expr(value interface{}) Expression {
	v, ok := value.(Expression)
	if ok {
		return v
	}
	return Expression{kind: literalKind, value: value}
}

// Array takes a series of items and converts them into an array.  This is just
// a convenience function for when you don't want to specify the types.
//
// Example usage:
//
//  founder1 := map[string]interface{}{"Name": "Slava Akhmechet"}
//  founder2 := map[string]interface{}{"Name": "Mike Glukhovsky"}
//  // Without Array:
//  r.Expr([]map[string]interface{}{founder1, founder2})
//  // With Array:
//  r.Array(founder1, founder2)
func Array(values ...interface{}) Expression {
	return Expr(values)
}

///////////
// Terms //
///////////

// JS creates an expression using Javascript code.  The code is executed
// on the server (using eval() https://developer.mozilla.org/en-US/docs/JavaScript/Reference/Global_Objects/eval)
// and can be used in a couple of roles, as value or as a function.  When used
// as a function, it received two named arguments, 'row' and/or 'acc' (used for
// reductions).
//
// The value of the 'this' object inside Javascript code is the current row.
//
// Example usage:
//
//  r.Table("employees").Map(r.JS(`this.first_name[0] + ' Fucking ' + this.last_name[0]`))
//  r.JS(`[1,2,3]`) // (same effect as r.Array(1,2,3))
//  r.JS(`({name: 2})`) // Parens are required here, otherwise eval() thinks it's a block.
func JS(body string) Expression {
	return Expression{kind: javascriptKind, value: body}
}

type letArgs struct {
	binds map[string]interface{}
	expr  interface{}
}

// Let binds a variable name to a value, then evaluates the given expression
// using the bindings it just made.  This is basically just assignment, but
// expressed in a way that works with the RQL language.
//
// Say you want something like this pseudo-javascript:
//
//  var results = [];
//  for (row in r.table("employees")) {
//      var joey = r.table("employees").get("joey");
//      results.push(row.awesomeness * joey.awesomeness);
//  }
//  return results;
//
// You can do the following RQL:
//
//  binds := map[string]interface{}{"joey": r.Table("employees").GetById("joey")}
//  expr := r.Row.Attr("awesomeness").Mul(r.LetVar("joey").Attr("awesomeness"))
//  r.Table("employees").Map(Let(binds, expr))
func Let(binds map[string]interface{}, expr interface{}) Expression {
	value := letArgs{
		binds: binds,
		expr:  expr,
	}
	return Expression{kind: letKind, value: value}
}

// LetVar lets you reference a variable bound with Let.
func LetVar(name string) Expression {
	return Expression{kind: variableKind, value: name}
}

// Error tells the server to respond with a RuntimeError, useful for testing.
func Error(message string) Expression {
	return Expression{kind: errorKind, value: message}
}

type ifArgs struct {
	test        interface{}
	trueBranch  interface{}
	falseBranch interface{}
}

// Branch checks a test expression, evaluating the trueBranch expression if it's
// true and falseBranch otherwise.
//
// Example usage:
//
//  // RQL expression
//  r.Branch(r.Row.Attr("first_name").Eq("Marc"), "is probably marc", "who cares")
//  // Equivalent Javascript expression
//  r.JS(`this.first_name == "Marc" ? "is probably marc" : "who cares"`)
func Branch(test, trueBranch, falseBranch interface{}) Expression {
	value := ifArgs{
		test:        test,
		trueBranch:  trueBranch,
		falseBranch: falseBranch,
	}
	return Expression{kind: ifKind, value: value}
}

type getArgs struct {
	table     interface{}
	key       Expression
	attribute string
}

// Get retrieves a single row by the named primary key (secondary key indexes are not
// supported yet by RethinkDB).
//
// Example usage:
//
//  r.Table("employees").Get("joey", "name")
func (e Expression) Get(key interface{}, attribute string) Expression {
	value := getArgs{table: e.value, key: Expr(key), attribute: attribute}
	return Expression{kind: getByKeyKind, value: value}
}

// GetById is the same as Get with "id" used as the attribute
//
// Example usage:
//
//  r.Table("employees").GetById("f001af8b-7d11-45a4-a268-a073ad4756ff", "id")
func (e Expression) GetById(key interface{}) Expression {
	return e.Get(key, "id")
}

type groupByArgs struct {
	attribute        string
	groupedMapReduce GroupedMapReduce
	expression       Expression
}

// GroupBy does a sort of grouped map reduce.  First the server groups all rows
// that have the same value for "attribute", then it applys the map reduce to
// each group.
//
// The GroupedMapReduce object can be one of the 3 supplied ones: r.Count(),
// r.Avg(attribute), r.Sum(attribute) or a user-built object:
//
//  gmr := r.GroupedMapReduce{
//      Mapping: r.JS(`this.awesomeness`),
//      Base: 0,
//      Reduction: r.JS(`acc + row`),
//      Finalizer: nil,
//  }
//  r.Table("employees").GroupBy("awesomeness", gmr)
func (e Expression) GroupBy(attribute string, groupedMapReduce GroupedMapReduce) Expression {
	return Expression{
		kind: groupByKind,
		value: groupByArgs{
			attribute:        attribute,
			groupedMapReduce: groupedMapReduce,
			expression:       e,
		},
	}
}

type useOutdatedArgs struct {
	expr        Expression
	useOutdated bool
}

// UseOutdated tells the server to use potentially out-of-date data from all
// tables already specified in this query.  The advantage is that read queries may be faster if this is set.
//
//  // Single table
//  r.Table("employees").UseOutdated(true)
//  // Entire query (all tables would be allowed to use outdated data)
//  r.Table("employees").Filter(Row.Attr("first_name").Eq("Joe")).UseOutdated(true)
func (e Expression) UseOutdated(useOutdated bool) Expression {
	value := useOutdatedArgs{expr: e, useOutdated: useOutdated}
	return Expression{kind: useOutdatedKind, value: value}
}

//////////////
// Builtins //
//////////////

type builtinArgs struct {
	operand interface{}
	args    []interface{}
}

func naryBuiltin(kind expressionKind, operand interface{}, args ...interface{}) Expression {
	return Expression{
		kind:  kind,
		value: builtinArgs{operand: operand, args: args},
	}
}

// Attr gets the attribute from the current row.
//
// Example usage:
//
//  Row.Attr("first_name")
func (e Expression) Attr(name string) Expression {
	return naryBuiltin(getAttributeKind, name, e)
}

// Add sums two numbers or concatenates two arrays
//
// Example usage:
//
//  r.Array(1,2,3).Add(r.Array(4,5,6))
//  r.Expr(2).Add(2)
func (e Expression) Add(operand interface{}) Expression {
	return naryBuiltin(addKind, nil, e, operand)
}

// Sub subtracts two numbers
//
// Example usage:
//
//  r.Expr(2).Sub(2)
func (e Expression) Sub(operand interface{}) Expression {
	return naryBuiltin(subtractKind, nil, e, operand)
}

// Mul multiplies two numbers
//
// Example usage:
//
//  r.Expr(2).Mul(2)
func (e Expression) Mul(operand interface{}) Expression {
	return naryBuiltin(multiplyKind, nil, e, operand)
}

// Div divides two numbers
//
// Example usage:
//
//  r.Expr(3).Div(2)
func (e Expression) Div(operand interface{}) Expression {
	return naryBuiltin(divideKind, nil, e, operand)
}

// Mod divides two numbers and returns the remainder
//
// Example usage:
//
//  r.Expr(23).Mod(10)
func (e Expression) Mod(operand interface{}) Expression {
	return naryBuiltin(moduloKind, nil, e, operand)
}

func (e Expression) And(operand interface{}) Expression {
	return naryBuiltin(logicalAndKind, nil, e, operand)
}

func (e Expression) Or(operand interface{}) Expression {
	return naryBuiltin(logicalOrKind, nil, e, operand)
}

func (e Expression) Eq(operand interface{}) Expression {
	return naryBuiltin(equalityKind, nil, e, operand)
}

func (e Expression) Ne(operand interface{}) Expression {
	return naryBuiltin(inequalityKind, nil, e, operand)
}

func (e Expression) Gt(operand interface{}) Expression {
	return naryBuiltin(greaterThanKind, nil, e, operand)
}

func (e Expression) Ge(operand interface{}) Expression {
	return naryBuiltin(greaterThanOrEqualKind, nil, e, operand)
}

func (e Expression) Lt(operand interface{}) Expression {
	return naryBuiltin(lessThanKind, nil, e, operand)
}

func (e Expression) Le(operand interface{}) Expression {
	return naryBuiltin(lessThanOrEqualKind, nil, e, operand)
}

func (e Expression) Not() Expression {
	return naryBuiltin(logicalNotKind, nil, e)
}

func (e Expression) ArrayToStream() Expression {
	return naryBuiltin(arrayToStreamKind, nil, e)
}

func (e Expression) StreamToArray() Expression {
	return naryBuiltin(streamToArrayKind, nil, e)
}

func (e Expression) Distinct() Expression {
	return naryBuiltin(distinctKind, nil, e)
}

func (e Expression) Count() Expression {
	return naryBuiltin(lengthKind, nil, e)
}

func (e Expression) Merge(operand interface{}) Expression {
	return naryBuiltin(mapMergeKind, nil, e, operand)
}

func (e Expression) Append(operand interface{}) Expression {
	return naryBuiltin(arrayAppendKind, nil, e, operand)
}

func (e Expression) Union(operands ...interface{}) Expression {
	return naryBuiltin(unionKind, nil, e, operands)
}

func (e Expression) Nth(operand interface{}) Expression {
	return naryBuiltin(nthKind, nil, e, operand)
}

func (e Expression) Slice(lower, upper interface{}) Expression {
	return naryBuiltin(sliceKind, nil, e, lower, upper)
}

func (e Expression) Limit(limit interface{}) Expression {
	return e.Slice(0, limit)
}

func (e Expression) Skip(start interface{}) Expression {
	return e.Slice(start, nil)
}

func (e Expression) Map(operand interface{}) Expression {
	return naryBuiltin(mapKind, operand, e)
}

func (e Expression) ConcatMap(operand interface{}) Expression {
	return naryBuiltin(concatMapKind, operand, e)
}

func (e Expression) Filter(operand interface{}) Expression {
	return naryBuiltin(filterKind, operand, e)
}

func (e Expression) Contains(key string) Expression {
	return naryBuiltin(hasAttributeKind, key, e)
}

func (e Expression) Pick(attributes ...string) Expression {
	return naryBuiltin(pickAttributesKind, attributes, e)
}

func (e Expression) Unpick(attributes ...string) Expression {
	return naryBuiltin(withoutKind, attributes, e)
}

type rangeArgs struct {
	attrname   string
	lowerbound interface{}
	upperbound interface{}
}

func (e Expression) Between(attrname string, lowerbound, upperbound interface{}) Expression {
	operand := rangeArgs{
		attrname:   attrname,
		lowerbound: lowerbound,
		upperbound: upperbound,
	}

	return naryBuiltin(rangeKind, operand, e)
}

func (e Expression) BetweenIds(lowerbound, upperbound interface{}) Expression {
	return e.Between("id", lowerbound, upperbound)
}

type orderByArgs struct {
	orderings []interface{}
}

func (e Expression) OrderBy(orderings ...interface{}) Expression {
	// These are not required to be strings because they could also be
	// orderByAttr structs which specify the direction of sorting
	operand := orderByArgs{
		orderings: orderings,
	}
	return naryBuiltin(orderByKind, operand, e)
}

type orderByAttr struct {
	attr      string
	ascending bool
}

func Asc(attr string) orderByAttr {
	return orderByAttr{attr, true}
}

func Desc(attr string) orderByAttr {
	return orderByAttr{attr, false}
}

type reduceArgs struct {
	base      interface{}
	reduction interface{}
}

func (e Expression) Reduce(base, reduction interface{}) Expression {
	operand := reduceArgs{
		base:      base,
		reduction: reduction,
	}
	return naryBuiltin(reduceKind, operand, e)
}

type groupedMapReduceArgs struct {
	grouping  interface{}
	mapping   interface{}
	base      interface{}
	reduction interface{}
}

func (e Expression) GroupedMapReduce(grouping, mapping, base, reduction interface{}) Expression {
	operand := groupedMapReduceArgs{
		grouping:  grouping,
		mapping:   mapping,
		base:      base,
		reduction: reduction,
	}
	return naryBuiltin(groupedMapReduceKind, operand, e)
}

/////////////////////
// Derived Methods //
/////////////////////

func (e Expression) Pluck(attributes ...string) Expression {
	return e.Map(Row.Pick(attributes...))
}

func (e Expression) Without(attributes ...string) Expression {
	return e.Map(Row.Unpick(attributes...))
}

type Predicate func(Expression, Expression) interface{}

func (leftExpr Expression) InnerJoin(rightExpr Expression, predicate Predicate) Expression {
	return leftExpr.ConcatMap(func(left Expression) interface{} {
		return rightExpr.ConcatMap(func(right Expression) interface{} {
			return Branch(predicate(left, right),
				Array(map[string]interface{}{"left": left, "right": right}),
				Array(),
			)
		})
	})
}

func (leftExpr Expression) OuterJoin(rightExpr Expression, predicate Predicate) Expression {
	// This is a left outer join
	return leftExpr.ConcatMap(func(left Expression) interface{} {
		return Let(map[string]interface{}{"matches": rightExpr.ConcatMap(func(right Expression) Expression {
			return Branch(
				predicate(left, right),
				Array(map[string]interface{}{"left": left, "right": right}),
				Array(),
			)
		}).StreamToArray()},
			Branch(
				LetVar("matches").Count().Gt(0),
				LetVar("matches"),
				Array(map[string]interface{}{"left": left}),
			))
	})
}

func (leftExpr Expression) EqJoin(leftAttribute string, rightExpr Expression, rightAttribute string) Expression {
	return leftExpr.ConcatMap(func(left Expression) interface{} {
		return Let(map[string]interface{}{"right": rightExpr.Get(left.Attr(leftAttribute), rightAttribute)},
			Branch(LetVar("right").Ne(nil),
				Array(map[string]interface{}{"left": left, "right": LetVar("right")}),
				Array(),
			))
	})
}

func (e Expression) Zip() Expression {
	return e.Map(func(row Expression) interface{} {
		return Branch(
			row.Contains("right"),
			row.Attr("left").Merge(row.Attr("right")),
			row.Attr("left"),
		)
	})
}

// GroupedMapReduce stores all the expressions needed to perform a .GroupBy()
// call, there are three pre-made ones: Count(), Sum(), and Avg().
type GroupedMapReduce struct {
	Mapping   interface{}
	Base      interface{}
	Reduction interface{}
	Finalizer interface{}
}

func Count() GroupedMapReduce {
	return GroupedMapReduce{
		Mapping:   func(row Expression) interface{} { return 1 },
		Base:      0,
		Reduction: func(acc, val Expression) interface{} { return acc.Add(val) },
	}
}

func Sum(attribute string) GroupedMapReduce {
	return GroupedMapReduce{
		Mapping:   func(row Expression) interface{} { return row.Attr(attribute) },
		Base:      0,
		Reduction: func(acc, val Expression) interface{} { return acc.Add(val) },
	}
}

func Avg(attribute string) GroupedMapReduce {
	return GroupedMapReduce{
		Mapping: func(row Expression) interface{} {
			return Array(row.Attr(attribute), 1)
		},
		Base: []int{0, 0},
		Reduction: func(acc, val Expression) interface{} {
			return []interface{}{
				acc.Nth(0).Add(val.Nth(0)),
				acc.Nth(1).Add(val.Nth(1)),
			}
		},
		Finalizer: func(row Expression) interface{} {
			return row.Nth(0).Div(row.Nth(1))
		},
	}
}

// Meta Queries
// Database administration (e.g. database create, table drop, etc)

type CreateDatabaseQuery struct {
	name string
}

// Create a database
func DBCreate(name string) Query {
	return Query{CreateDatabaseQuery{name}}
}

type DropDatabaseQuery struct {
	name string
}

// Drop database
func DBDrop(name string) Query {
	return Query{DropDatabaseQuery{name}}
}

type ListDatabasesQuery struct{}

// List all databases
func DBList() Query {
	return Query{ListDatabasesQuery{}}
}

type Database struct {
	name string
}

func Db(name string) Database {
	return Database{name}
}

type TableCreateQuery struct {
	name     string
	database Database

	// These can be set by the user
	PrimaryKey        string
	PrimaryDatacenter string
	CacheSize         int64
}

func (db Database) TableCreate(name string) Query {
	return Query{TableCreateQuery{name: name, database: db}}
}

type TableListQuery struct {
	database Database
}

// List all tables in this database
func (db Database) TableList() Query {
	return Query{TableListQuery{db}}
}

type TableDropQuery struct {
	table TableInfo
}

// Drop a table from a database
func (db Database) TableDrop(name string) Query {
	table := TableInfo{
		name:     name,
		database: db,
	}
	return Query{TableDropQuery{table: table}}
}

type TableInfo struct {
	name     string
	database Database
}

func (db Database) Table(name string) Expression {
	value := TableInfo{
		name:     name,
		database: db,
	}
	return Expression{kind: tableKind, value: value}
}

func Table(name string) Expression {
	value := TableInfo{
		name: name,
	}
	return Expression{kind: tableKind, value: value}
}

// Write Queries

type InsertQuery struct {
	tableExpr Expression
	rows      []interface{}
	overwrite bool
}

func (e Expression) Insert(rows ...interface{}) Query {
	// Assume the expression is a table for now, we'll check later in buildProtobuf
	return Query{InsertQuery{
		tableExpr: e,
		rows:      rows,
		overwrite: false,
	}}
}

// TODO: how to make this work - could make it runtime type-assert Query
// could also have a .Run() specifically defined for InsertQuery
// could also have .InsertOverwrite() or .Overwrite() instead of .Insert()
// func (q InsertQuery) Overwrite(overwrite bool) InsertQuery {
//  q.overwrite = overwrite
//  return q
// }

type UpdateQuery struct {
	view    Expression
	mapping interface{}
}

func (e Expression) Update(mapping interface{}) Query {
	return Query{UpdateQuery{
		view:    e,
		mapping: mapping,
	}}
}

type ReplaceQuery struct {
	view    Expression
	mapping interface{}
}

func (e Expression) Replace(mapping interface{}) Query {
	return Query{ReplaceQuery{
		view:    e,
		mapping: mapping,
	}}
}

type DeleteQuery struct {
	view Expression
}

func (e Expression) Delete() Query {
	return Query{DeleteQuery{view: e}}
}

type ForEachQuery struct {
	stream    Expression
	queryFunc func(Expression) RethinkQuery
}

func (e Expression) ForEach(queryFunc (func(Expression) RethinkQuery)) Query {
	return Query{ForEachQuery{stream: e, queryFunc: queryFunc}}
}
