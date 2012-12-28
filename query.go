package rethinkgo

// Let user create queries as RQL Expression trees, any errors are deferred
// until the query is run, so most all functions take interface{} types.
// interface{} is effectively a void* type that we look at later to determine
// the underlying type and perform any conversions.

type Map map[string]interface{}
type List []interface{}

type expressionKind int

const (
	// These I just made up
	literalKind expressionKind = iota // converted to an Expression
	functionKind
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
	// implicitGetAttributeKind - appears to be unused
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

// Expression represents an RQL expression, such as .Filter(), which, when called on
// another expression, filters the results of that expression when run on the
// server.  It is used as the argument type of any functions used in RQL.
//
// To create an expression from a native type, or user-defined type, or
// function, use Expr().  In most cases, this is not necessary as conversion to
// an expression will be done automatically.
//
// Example usage:
//
//  var response []interface{}
//  err := r.Table("heroes").Map(func(row r.Expression) r.Expression {
//      return row.Attr("intelligence")
//  }).Run().Collect(&response)
//
// Example response:
//
//  [7, 5, 4, 6, 2, 2, 6, 4, ...]
//
// Expressions can be used directly as queries, they are convered to read
// queries that just return whatever rows have been selected.
//
// Example usage:
//
//  var response []interface{}
//  err := r.Table("heroes").Filter(r.Row.Attr("intelligence").Eq(7)).Run().Collect(&response)
//
// Example response:
//
//  [
//    {
//		"strength": 7,
//		"name": "Jean Grey",
//		"durability": 7,
//		"intelligence": 7,
//		"energy": 7,
//		"fighting": 7,
//		"real_name": "Jean Grey-Summers",
//		"speed": 7,
//		"id": "c073673f-8b33-4698-a2ec-62b18a3e8c4f"
//	  },
//    ...
//  ]
type Expression struct {
	value interface{}
	kind  expressionKind
}

// WriteQuery is the type returned by any method that writes to a table, this
// includes .Insert(), .Update(), .Delete(), .ForEach(), and .Replace().  The
// message returned by the server for these queries can be read into the
// r.WriteResponse struct.
type WriteQuery struct {
	query     interface{}
	nonatomic bool
	overwrite bool // for insert query
}

// MetaQuery is the type returned by methods that create/modify/delete
// databases, this includes .TableCreate(), .TableList(), .TableDrop(),
// .DbCreate(), .DbList(), and .DbDrop().  These return empty responses, except
// for .*List() which return []string.
type MetaQuery struct {
	query interface{}
}

// Row supplies access to the current row in any query, even if there's no go
// func with a reference to it.
//
// Example without Row:
//
//  var response []interface{}
//  err := r.Table("villains").Map(func(row r.Expression) r.Expression {
//      return row.Attr("real_name")
//  }).Run().Collect(&response)
//
// Example with Row:
//
//  var response []interface{}
//  err := r.Table("employees").Map(Row.Attr("real_name")).Run().Collect(&response)
//
// Example response:
//
//  ["En Sabah Nur", "Victor von Doom", ...]
var Row = Expression{kind: implicitVariableKind}

// Expr converts any value to an expression.  Internally it uses the `json`
// module to convert any literals, so any type annotations or methods understood
// by that module can be used. If the value cannot be converted, an error is
// returned at query .Run() time.
//
// If you want to call expression methods on an object that is not yet an
// expression, this is the function you want.
//
// Example usage:
//
//  var response interface{}
//  rows := r.Expr(r.Map{"go": "awesome", "rethinkdb": "awesomer"}).Run().One(&response)
//
// Example response:
//
//  {"go": "awesome", "rethinkdb": "awesomer"}
func Expr(values ...interface{}) Expression {
	switch len(values) {
	case 0:
		return Expression{kind: literalKind, value: nil}
	case 1:
		value := values[0]
		v, ok := value.(Expression)
		if ok {
			return v
		}
		return Expression{kind: literalKind, value: value}
	}
	return Expression{kind: literalKind, value: values}
}

///////////
// Terms //
///////////

// Js creates an expression using Javascript code.  The code is executed
// on the server (using eval() https://developer.mozilla.org/en-US/docs/JavaScript/Reference/Global_Objects/eval)
// and can be used in a couple of roles, as value or as a function.  When used
// as a function, it receives two named arguments, 'row' and/or 'acc' (used for
// reductions).
//
// The value of the 'this' object inside Javascript code is the current row.
//
// Example usage:
//
//  r.Table("employees").Map(r.Js(`this.first_name[0] + ' Fucking ' + this.last_name[0]`)).Run()
//  r.Js(`[1,2,3]`).Run() // (same effect as r.Expr(1,2,3))
//  r.Js(`({name: 2})`).Run() // Parens are required here, otherwise eval() thinks it's a block.
//
// Example without Js:
//
//  var response []interface{}
//  rows := r.Table("heroes").Map(func(row r.Expression) r.Expression {
//      return row.Attr("strength").Add(row.Attr("durability"))
//  }).Run().Collect(&response)
//
// Example with Js:
//
//  var response []interface{}
//  rows := r.Table("heroes").Map(
//      r.Js(`this.strength + this.durability`)
//  ).Run().Collect(&response)
//
// Example response:
//
//  [11, 6, 9, 11, ...]
//
// When using a Js call inside of a function that is compiled into RQL, the
// variable names inside the javascript are not the same as in Go.  To get
// around this, you can convert the variable to a string and use that in the Js
// code.
//
// Example inside a function:
//
//  var response []interface{}
//  // Find each hero-villain pair with the same strength
//  rows := r.Table("heroes").InnerJoin(r.Table("villains"), func(hero, villain r.Expression) r.Expression {
//      return r.Js(fmt.Sprintf("%v.strength == %v.strength", hero, villain))
//  }).Run().Collect(&response)
//
// Example response:
//
// [
//   {
//     "left":
//     {
//       "durability": 5,
//       "energy": 7,
//       "fighting": 7,
//       "id": "f915d9a7-6cfa-4151-b5f6-6aded7da595f",
//       "intelligence": 5,
//       "name": "Nightcrawler",
//       "real_name": "Kurt Wagner",
//       "speed": 7,
//       "strength": 4
//     },
//     "right":
//     {
//       "durability": 4,
//       "energy": 1,
//       "fighting": 7,
//       "id": "12e58b11-93b3-4e89-987d-efb345001dfe",
//       "intelligence": 2,
//       "name": "Sabretooth",
//       "real_name": "Victor Creed",
//       "speed": 2,
//       "strength": 4
//     }
//   },
//   ...
// ]
func Js(body string) Expression {
	return Expression{kind: javascriptKind, value: body}
}

type fnArgs struct {
	args string
	body interface{}
}

// Fn creates a function expression that takes the specified args, this is
// useful in some cases, for instance, if you want to create a function body
// that uses Js().  Without this, automatic variable names are generated and
// need to be interpolated into the javascript.  Variable names should be
// separated with ", ".
//
// TODO: this example doesn't work because inner/outerjoin require actual functions
// TODO: remove Fn? already have func and Row and %v
//
// Example without Fn:
//
//  rows := r.Table("heroes").InnerJoin(r.Table("villains"), func(hero, villain r.Expression) r.Expression {
//      return r.Js(fmt.Sprintf("%v.strength == %v.strength", hero, villain))
//  }).Run()
//
// Example with Fn:
//
//  rows := r.Table("heroes").InnerJoin(r.Table("villains"), r.Fn("hero, villain", r.Js(`hero.strength == villain.strength`))).Run()
func Fn(args string, body interface{}) Expression {
	value := fnArgs{
		args: args,
		body: body,
	}
	return Expression{kind: functionKind, value: value}
}

type letArgs struct {
	binds map[string]interface{}
	expr  interface{}
}

// Let binds a variable name to a value, then evaluates the given expression
// using the bindings it just made.  This is basically just assignment, but
// expressed in a way that works with RQL.
//
// Say you want something like this pseudo-javascript:
//
// TODO: this example sucks
//
//  var results = [];
//  for (row in r.table("")) {
//      var joey = r.table("employees").get("joey");
//      results.push(row.awesomeness * joey.awesomeness);
//  }
//  return results;
//
// You can do that with the following RQL:
//
//  binds := r.Map{"joey": r.Table("employees").GetById("joey")}
//  expr := r.Row.Attr("awesomeness").Mul(r.LetVar("joey").Attr("awesomeness"))
//  r.Table("employees").Map(Let(binds, expr))
func Let(binds Map, expr interface{}) Expression {
	value := letArgs{
		binds: binds,
		expr:  expr,
	}
	return Expression{kind: letKind, value: value}
}

// LetVar lets you reference a variable bound in the current context (for
// example, with Let()).  See the Let example for how to use LetVar.
func LetVar(name string) Expression {
	return Expression{kind: variableKind, value: name}
}

// RuntimeError tells the server to respond with a RuntimeError, useful for
// testing.
//
// Example usage:
//
//  err := r.RuntimeError("hi there").Run().Err()
func RuntimeError(message string) Expression {
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
//  // Roughly equivalent Javascript expression
//  r.Js(`this.first_name == "Marc" ? "is probably marc" : "who cares"`)
func Branch(test, trueBranch, falseBranch interface{}) Expression {
	value := ifArgs{
		test:        test,
		trueBranch:  trueBranch,
		falseBranch: falseBranch,
	}
	return Expression{kind: ifKind, value: value}
}

type getArgs struct {
	table     Expression
	key       Expression
	attribute string
}

// Get retrieves a single row by the named primary key (secondary key indexes are not
// supported yet by RethinkDB).
//
// Example usage:
//
//  var response map[string]interface{}
//  err := r.Table("heroes").Get("Doctor Strange", "name").Run().One(&response)
//
// Example response:
//
//  {
//    "strength": 3,
//    "name": "Doctor Strange",
//    "durability": 6,
//    "intelligence": 4,
//    "energy": 7,
//    "fighting": 7,
//    "real_name": "Stephen Vincent Strange",
//    "speed": 5,
//    "id": "edc3a46b-95a0-4f64-9d1c-0dd7d83c4bcd"
//  }
func (e Expression) Get(key interface{}, attribute string) Expression {
	value := getArgs{table: e, key: Expr(key), attribute: attribute}
	return Expression{kind: getByKeyKind, value: value}
}

// GetById is the same as Get with "id" used as the attribute
//
// Example usage:
//
//  var response map[string]interface{}
//  err := r.Table("heroes").GetById("edc3a46b-95a0-4f64-9d1c-0dd7d83c4bcd").Run().One(&response)
//
// Example response:
//
//  {
//    "strength": 3,
//    "name": "Doctor Strange",
//    "durability": 6,
//    "intelligence": 4,
//    "energy": 7,
//    "fighting": 7,
//    "real_name": "Stephen Vincent Strange",
//    "speed": 5,
//    "id": "edc3a46b-95a0-4f64-9d1c-0dd7d83c4bcd"
//  }
func (e Expression) GetById(key interface{}) Expression {
	return e.Get(key, "id")
}

type groupByArgs struct {
	attribute        interface{}
	groupedMapReduce GroupedMapReduce
	expr             Expression
}

// GroupBy does a sort of grouped map reduce.  First the server groups all rows
// that have the same value for `attribute`, then it applys the map reduce to
// each group.  It takes a GroupedMapReduce object that specifies how to do the
// map reduce.
//
// The GroupedMapReduce object can be one of the 3 supplied ones: r.Count(),
// r.Avg(attribute), r.Sum(attribute) or a user-supplied object:
//
// Example usage:
//
//  // Find all heroes with the same strength, sum their intelligence
//  gmr := r.GroupedMapReduce{
//      Mapping: func(row r.Expression) r.Expression { return row.Attr("intelligence") },
//      Base: 0,
//      Reduction: func(acc, val r.Expression) r.Expression { return acc.Add(val) },
//      Finalizer: nil,
//  }
//  var response []interface{}
//  err := r.Table("heroes").GroupBy("strength", gmr).Run().Collect(&response)
//
// Example response:
//
//  [
//    {
//      "group": 1, // this is the strength attribute for every member of this group
//      "reduction": 2  // this is the sum of the intelligence attribute of all members of the group
//    },
//    {
//      "group": 2,
//      "reduction": 15
//    },
//    ...
//  ]
//
// `attribute` must be a single attribute (string) or a list of attributes
// ([]string)
//
// Example with multiple attributes:
//
//  rows := r.Table("heroes").GroupBy([]string{"strength", "speed"}, gmr).Run()
func (e Expression) GroupBy(attribute interface{}, groupedMapReduce GroupedMapReduce) Expression {
	return Expression{
		kind: groupByKind,
		value: groupByArgs{
			attribute:        attribute,
			groupedMapReduce: groupedMapReduce,
			expr:             e,
		},
	}
}

type useOutdatedArgs struct {
	expr        Expression
	useOutdated bool
}

// UseOutdated tells the server to use potentially out-of-date data from all
// tables already specified in this query. The advantage is that read queries
// may be faster if this is set.
//
// Example with single table:
//
//  rows := r.Table("heroes").UseOutdated(true).Run()
//
// Example with multiple tables (all tables would be allowed to use outdated data):
//
//  villain_strength := r.Table("villains").Get("Doctor Doom", "name").Attr("strength")
//  rows := r.Table("heroes").Filter(r.Row.Attr("strength").Eq(villain_strength)).UseOutdated(true).Run()
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

// Attr gets an attribute's value from the row.
//
// Example usage:
//
//  r.Expr(r.Map{"key": "value"}).Attr("key") => "value"
func (e Expression) Attr(name string) Expression {
	return naryBuiltin(getAttributeKind, name, e)
}

// Add sums two numbers or concatenates two arrays.
//
// Example usage:
//
//  r.Expr(1,2,3).Add(r.Expr(4,5,6)) => [1,2,3,4,5,6]
//  r.Expr(2).Add(2) => 4
func (e Expression) Add(operand interface{}) Expression {
	return naryBuiltin(addKind, nil, e, operand)
}

// Sub subtracts two numbers.
//
// Example usage:
//
//  r.Expr(2).Sub(2) => 0
func (e Expression) Sub(operand interface{}) Expression {
	return naryBuiltin(subtractKind, nil, e, operand)
}

// Mul multiplies two numbers.
//
// Example usage:
//
//  r.Expr(2).Mul(3) => 6
func (e Expression) Mul(operand interface{}) Expression {
	return naryBuiltin(multiplyKind, nil, e, operand)
}

// Div divides two numbers.
//
// Example usage:
//
//  r.Expr(3).Div(2) => 1.5
func (e Expression) Div(operand interface{}) Expression {
	return naryBuiltin(divideKind, nil, e, operand)
}

// Mod divides two numbers and returns the remainder.
//
// Example usage:
//
//  r.Expr(23).Mod(10) => 3
func (e Expression) Mod(operand interface{}) Expression {
	return naryBuiltin(moduloKind, nil, e, operand)
}

// And performs a logical and on two values.
//
// Example usage:
//
//  r.Expr(true).And(true) => true
func (e Expression) And(operand interface{}) Expression {
	return naryBuiltin(logicalAndKind, nil, e, operand)
}

// Or performs a logical or on two values.
//
// Example usage:
//
//  r.Expr(true).Or(false) => true
func (e Expression) Or(operand interface{}) Expression {
	return naryBuiltin(logicalOrKind, nil, e, operand)
}

// Eq returns true if two values are equal.
//
// Example usage:
//
//  r.Expr(1).Eq(1) => true
func (e Expression) Eq(operand interface{}) Expression {
	return naryBuiltin(equalityKind, nil, e, operand)
}

// Ne returns true if two values are not equal.
//
// Example usage:
//
//  r.Expr(1).Ne(-1) => true
func (e Expression) Ne(operand interface{}) Expression {
	return naryBuiltin(inequalityKind, nil, e, operand)
}

// Gt returns true if the first value is greater than the second.
//
// Example usage:
//
//  r.Expr(2).Gt(1) => true
func (e Expression) Gt(operand interface{}) Expression {
	return naryBuiltin(greaterThanKind, nil, e, operand)
}

// Gt returns true if the first value is greater than or equal to the second.
//
// Example usage:
//
//  r.Expr(2).Gt(2) => true
func (e Expression) Ge(operand interface{}) Expression {
	return naryBuiltin(greaterThanOrEqualKind, nil, e, operand)
}

// Lt returns true if the first value is less than the second.
//
// Example usage:
//
//  r.Expr(1).Lt(2) => true
func (e Expression) Lt(operand interface{}) Expression {
	return naryBuiltin(lessThanKind, nil, e, operand)
}

// Le returns true if the first value is less than or equal to the second.
//
// Example usage:
//
//  r.Expr(2).Lt(2) => true
func (e Expression) Le(operand interface{}) Expression {
	return naryBuiltin(lessThanOrEqualKind, nil, e, operand)
}

// Not performs a logical not on a value.
//
// Example usage:
//
//  r.Expr(2).Lt(2) => true
func (e Expression) Not() Expression {
	return naryBuiltin(logicalNotKind, nil, e)
}

// ArrayToStream converts an array of objects to a stream.  Many operators work
// on both streams and arrays. .Union() requires that both operands be the same
// type.
//
// Example with array (note use of .One()):
//
//  var response []interface{}
//  err := r.Expr(1,2,3).Run().One(&response) => [1, 2, 3]
//
// Example with stream (note use of .Collect()):
//
//  var response []interface{}
//  err := r.Expr(1,2,3).ArrayToStream().Run().Collect(&response) => [1, 2, 3]
//
// Example with .Union():
//
//  var response []interface{}
//  r.Expr(1,2,3,4).ArrayToStream().Union(r.Table("heroes")).Run().Collect(&response)
func (e Expression) ArrayToStream() Expression {
	return naryBuiltin(arrayToStreamKind, nil, e)
}

// StreamToArray converts an stream of objects into an array.  Many operators
// work on both streams and arrays. .Union() requires that both operands be the
// same type.
//
// Example with stream (note use of .Collect()):
//
//  var response []interface{}
//  err := r.Table("heroes").Run().Collect(&response) => [{hero...}, {hero...}, ...]
//
// Example with array (note use of .One()):
//
//  var response []interface{}
//  err := r.Table("heroes").StreamToArray().Run().One(&response) => [{hero...}, {hero...}, ...]
//
// Example with .Union():
//
//  var response []interface{}
//  r.Expr(1,2,3,4).Union(r.Table("heroes").StreamToArray()).Run().One(&response)
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
	// elements of operands should be among the rest of the args to the UNION call,
	// at the same level as the expression
	// table.Union(table) needs to end up being
	// UNION Args: [table, table]
	// instead of
	// UNION Args: [table, [table]]
	// so flatten the list of operands
	args := []interface{}{e}
	args = append(args, operands...)
	return naryBuiltin(unionKind, nil, args...)
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

// TODO: example with fetching by id
func (e Expression) Map(operand interface{}) Expression {
	return naryBuiltin(mapKind, operand, e)
}

func (e Expression) ConcatMap(operand interface{}) Expression {
	return naryBuiltin(concatMapKind, operand, e)
}

func (e Expression) Filter(operand interface{}) Expression {
	return naryBuiltin(filterKind, operand, e)
}

func (e Expression) Contains(keys ...string) Expression {
	expr := Expr(true)
	for _, key := range keys {
		expr = expr.And(naryBuiltin(hasAttributeKind, key, e))
	}
	return expr
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

func (leftExpr Expression) InnerJoin(rightExpr Expression, predicate func(Expression, Expression) Expression) Expression {
	return leftExpr.ConcatMap(func(left Expression) interface{} {
		return rightExpr.ConcatMap(func(right Expression) interface{} {
			return Branch(predicate(left, right),
				List{Map{"left": left, "right": right}},
				List{},
			)
		})
	})
}

func (leftExpr Expression) OuterJoin(rightExpr Expression, predicate func(Expression, Expression) Expression) Expression {
	// This is a left outer join
	return leftExpr.ConcatMap(func(left Expression) interface{} {
		return Let(Map{"matches": rightExpr.ConcatMap(func(right Expression) Expression {
			return Branch(
				predicate(left, right),
				List{Map{"left": left, "right": right}},
				List{},
			)
		}).StreamToArray()},
			Branch(
				LetVar("matches").Count().Gt(0),
				LetVar("matches"),
				List{Map{"left": left}},
			))
	})
}

func (leftExpr Expression) EqJoin(leftAttribute string, rightExpr Expression, rightAttribute string) Expression {
	return leftExpr.ConcatMap(func(left Expression) interface{} {
		return Let(Map{"right": rightExpr.Get(left.Attr(leftAttribute), rightAttribute)},
			Branch(LetVar("right").Ne(nil),
				List{Map{"left": left, "right": LetVar("right")}},
				List{},
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
			return List{row.Attr(attribute), 1}
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

type createDatabaseQuery struct {
	name string
}

// DbCreate create a database with the supplied name.
//
// Example usage:
//
//  _, err := r.DbCreate("company").Run()
//
// Example response:
//
//  nil
func DbCreate(name string) MetaQuery {
	return MetaQuery{query: createDatabaseQuery{name}}
}

type dropDatabaseQuery struct {
	name string
}

// DbDrop deletes the specified database
//
// Example usage:
//
//  _, err := r.DbDrop("company").Run()
//
// Example response:
//
//  nil
func DbDrop(name string) MetaQuery {
	return MetaQuery{query: dropDatabaseQuery{name}}
}

type listDatabasesQuery struct{}

// DbList lists all databases on the server
//
// Example usage:
//
//  rows, err := r.DbList().Run()
//  var databases []string
//  err = rows.Collect(&databases)
//
// Example response:
//
//  []string{"test", "company"}
func DbList() MetaQuery {
	return MetaQuery{query: listDatabasesQuery{}}
}

type database struct {
	name string
}

// Db lets you perform operations within a specific database (this will override
// the database specified to the connection).  This can be used to access or
// create/list/delete tables within any database available on the server.
func Db(name string) database {
	return database{name}
}

type tableCreateQuery struct {
	database database
	spec     TableSpec
}

// TableSpec lets you specify the various parameters for a table, then create it
// with TableCreateSpec().
type TableSpec struct {
	Name              string
	PrimaryKey        string
	PrimaryDatacenter string
	CacheSize         int64
}

// TableCreate creates a table with the specified name.
//
// Example usage:
//
//  _, err := r.Db("company").TableCreate("employees").Run()
//
// Example response:
//
//  nil
func (db database) TableCreate(name string) MetaQuery {
	spec := TableSpec{Name: name}
	return MetaQuery{query: tableCreateQuery{spec: spec, database: db}}
}

// TableCreateSpec creates a table with the specified attributes.
//
// Example usage:
//
//  spec := TableSpec{Name: "employees", PrimaryKey: "userid"}
//  _, err := r.Db("company").TableCreateSpec(spec).Run()
//
// Example response:
//
//  nil
func (db database) TableCreateSpec(spec TableSpec) MetaQuery {
	return MetaQuery{query: tableCreateQuery{spec: spec, database: db}}
}

type tableListQuery struct {
	database database
}

// List all tables in this database
func (db database) TableList() MetaQuery {
	return MetaQuery{query: tableListQuery{db}}
}

type tableDropQuery struct {
	table tableInfo
}

// Drop a table from a database
func (db database) TableDrop(name string) MetaQuery {
	table := tableInfo{
		name:     name,
		database: db,
	}
	return MetaQuery{query: tableDropQuery{table: table}}
}

type tableInfo struct {
	name     string
	database database
}

func (db database) Table(name string) Expression {
	value := tableInfo{
		name:     name,
		database: db,
	}
	return Expression{kind: tableKind, value: value}
}

func Table(name string) Expression {
	value := tableInfo{
		name: name,
	}
	return Expression{kind: tableKind, value: value}
}

type insertQuery struct {
	tableExpr Expression
	rows      []interface{}
}

func (e Expression) Insert(rows ...interface{}) WriteQuery {
	// Assume the expression is a table for now, we'll check later in buildProtobuf
	return WriteQuery{query: insertQuery{
		tableExpr: e,
		rows:      rows,
	}}
}

func (q WriteQuery) Overwrite(overwrite bool) WriteQuery {
	q.overwrite = overwrite
	return q
}

func (q WriteQuery) Atomic(atomic bool) WriteQuery {
	q.nonatomic = !atomic
	return q
}

type updateQuery struct {
	view    Expression
	mapping interface{}
}

func (e Expression) Update(mapping interface{}) WriteQuery {
	return WriteQuery{query: updateQuery{
		view:    e,
		mapping: mapping,
	}}
}

type replaceQuery struct {
	view    Expression
	mapping interface{}
}

func (e Expression) Replace(mapping interface{}) WriteQuery {
	return WriteQuery{query: replaceQuery{
		view:    e,
		mapping: mapping,
	}}
}

type deleteQuery struct {
	view Expression
}

func (e Expression) Delete() WriteQuery {
	return WriteQuery{query: deleteQuery{view: e}}
}

type forEachQuery struct {
	stream    Expression
	queryFunc func(Expression) Query
}

func (e Expression) ForEach(queryFunc (func(Expression) Query)) WriteQuery {
	return WriteQuery{query: forEachQuery{stream: e, queryFunc: queryFunc}}
}
