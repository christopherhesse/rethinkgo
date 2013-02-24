package rethinkgo

// Let user create queries as RQL Exp trees, any errors are deferred
// until the query is run, so most all functions take interface{} types.
// interface{} is effectively a void* type that we look at later to determine
// the underlying type and perform any conversions.

// Map is a shorter name for a mapping from strings to arbitrary objects
type Map map[string]interface{}

// List is a shorter name for an array of arbitrary objects
type List []interface{}

type expressionKind int

const (
	// These I just made up
	literalKind expressionKind = iota // converted to an Exp
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

// Exp represents an RQL expression, such as the return value of
// r.Expr(). Exp has all the RQL methods on it, such as .Add(), .Attr(),
// .Filter() etc.
//
// To create an Exp from a native type, or user-defined type, or
// function, use r.Expr().
//
// Example usage:
//
//  r.Expr(2).Mul(2) => 4
//
// Exp is the type used for the arguments to any functions that are used
// in RQL.
//
// Example usage:
//
//  var response []interface{}
//  // Get the intelligence rating for each of our heroes
//  getIntelligence := func(row r.Exp) r.Exp {
//      return row.Attr("intelligence")
//  }
//  err := r.Table("heroes").Map(getIntelligence).Run(session).Collect(&response)
//
// Example response:
//
//  [7, 5, 4, 6, 2, 2, 6, 4, ...]
//
// Literal Expressions can be used directly for queries.
//
// Example usage:
//
//  var squares []int
//  // Square a series of numbers
//  square := func(row r.Exp) r.Exp { return row.Mul(row) }
//  err := r.Expr(1,2,3).Map(square).Run(session).One(&squares)
//
// Example response:
//
//  [1, 2, 3]
type Exp struct { // this would be Expr, but then it would conflict with the function that creates Exp instances
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
// for .*List() functions, which return []string.
type MetaQuery struct {
	query interface{}
}

// Row supplies access to the current row in any query, even if there's no go
// func with a reference to it.
//
// Example without Row:
//
//  var response []interface{}
//  // Get the real names of all the villains
//  err := r.Table("villains").Map(func(row r.Exp) r.Exp {
//      return row.Attr("real_name")
//  }).Run(session).Collect(&response)
//
// Example with Row:
//
//  var response []interface{}
//  // Get the real names of all the villains
//  err := r.Table("employees").Map(Row.Attr("real_name")).Run(session).Collect(&response)
//
// Example response:
//
//  ["En Sabah Nur", "Victor von Doom", ...]
var Row = Exp{kind: implicitVariableKind}

// Expr converts any value to an expression.  Internally it uses the `json`
// module to convert any literals, so any type annotations or methods understood
// by that module can be used. If the value cannot be converted, an error is
// returned at query .Run(session) time.
//
// If you want to call expression methods on an object that is not yet an
// expression, this is the function you want.
//
// Example usage:
//
//  var response interface{}
//  rows := r.Expr(r.Map{"go": "awesome", "rethinkdb": "awesomer"}).Run(session).One(&response)
//
// Example response:
//
//  {"go": "awesome", "rethinkdb": "awesomer"}
func Expr(values ...interface{}) Exp {
	switch len(values) {
	case 0:
		return Exp{kind: literalKind, value: nil}
	case 1:
		value := values[0]
		v, ok := value.(Exp)
		if ok {
			return v
		}
		return Exp{kind: literalKind, value: value}
	}
	return Exp{kind: literalKind, value: values}
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
// Example usages:
//
//  // (same effect as r.Expr(1,2,3))
//  r.Js(`[1,2,3]`).Run(session)
//  // Parens are required here, otherwise eval() thinks it's a block.
//  r.Js(`({name: 2})`).Run(session)
//  // String concatenation is possible using r.Js
//  r.Table("employees").Map(r.Js(`this.first_name[0] + ' Fucking ' + this.last_name[0]`)).Run(session)
//
// Example without Js:
//
//  var response []interface{}
//  // Combine each hero's strength and durability
//  rows := r.Table("heroes").Map(func(row r.Exp) r.Exp {
//      return row.Attr("strength").Add(row.Attr("durability"))
//  }).Run(session).Collect(&response)
//
// Example with Js:
//
//  var response []interface{}
//  // Combine each hero's strength and durability
//  rows := r.Table("heroes").Map(
//      r.Js(`this.strength + this.durability`)
//  ).Run(session).Collect(&response)
//
// Example response:
//
//  [11, 6, 9, 11, ...]
//
// When using a Js call inside of a function that is compiled into RQL, the
// variable names inside the javascript are not the same as in Go.  To access
// the variable, you can convert the variable to a string (it will become the
// name of the variable) and use that in the Js code.
//
// Example inside a function:
//
//  var response []interface{}
//  // Find each hero-villain pair with the same strength
//  err := r.Table("heroes").InnerJoin(r.Table("villains"), func(hero, villain r.Exp) r.Exp {
//      return r.Js(fmt.Sprintf("%v.strength == %v.strength", hero, villain))
//  }).Run(session).Collect(&response)
//
// Example response:
//
//  [
//    {
//      "left":
//      {
//        "durability": 5,
//        "energy": 7,
//        "fighting": 7,
//        "id": "f915d9a7-6cfa-4151-b5f6-6aded7da595f",
//        "intelligence": 5,
//        "name": "Nightcrawler",
//        "real_name": "Kurt Wagner",
//        "speed": 7,
//        "strength": 4
//      },
//      "right":
//      {
//        "durability": 4,
//        "energy": 1,
//        "fighting": 7,
//        "id": "12e58b11-93b3-4e89-987d-efb345001dfe",
//        "intelligence": 2,
//        "name": "Sabretooth",
//        "real_name": "Victor Creed",
//        "speed": 2,
//        "strength": 4
//      }
//    },
//    ...
//   ]
func Js(body string) Exp {
	return Exp{kind: javascriptKind, value: body}
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
//  var results = [];
//  var havok = r.table("heroes").get("havok", "name");
//  for (villain in r.table("villains")) {
//      results.push(villain.strength > havok.strength);
//  }
//  return results;
//
// You can do that with the following RQL:
//
//  var response []bool
//  // For each villain, check if that villain is stronger than Havok
//  binds := r.Map{"havok": r.Table("heroes").Get("Havok", "name")}
//  expr := r.Row.Attr("strength").Gt(r.LetVar("havok").Attr("strength"))
//  query := r.Table("villains").Map(r.Let(binds, expr))
//  err := query.Run(session).Collect(&response)
//
// Example response:
//
//  [true, true, true, false, false, ...]
func Let(binds Map, expr interface{}) Exp {
	value := letArgs{
		binds: binds,
		expr:  expr,
	}
	return Exp{kind: letKind, value: value}
}

// LetVar lets you reference a variable bound in the current context (for
// example, with Let()).  See the Let example for how to use LetVar.
func LetVar(name string) Exp {
	return Exp{kind: variableKind, value: name}
}

// RuntimeError tells the server to respond with a ErrRuntime, useful for
// testing.
//
// Example usage:
//
//  err := r.RuntimeError("hi there").Run(session).Err()
func RuntimeError(message string) Exp {
	return Exp{kind: errorKind, value: message}
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
//  // Javascript expression
//  r.Js(`this.first_name == "Marc" ? "is probably marc" : "who cares"`)
//  // Roughly equivalent RQL expression
//  r.Branch(r.Row.Attr("first_name").Eq("Marc"), "is probably marc", "who cares")
func Branch(test, trueBranch, falseBranch interface{}) Exp {
	value := ifArgs{
		test:        test,
		trueBranch:  trueBranch,
		falseBranch: falseBranch,
	}
	return Exp{kind: ifKind, value: value}
}

type getArgs struct {
	table     Exp
	key       Exp
	attribute string
}

// Get retrieves a single row by the named primary key (secondary key indexes are not
// supported yet by RethinkDB).
//
// Example usage:
//
//  var response map[string]interface{}
//  err := r.Table("heroes").Get("Doctor Strange", "name").Run(session).One(&response)
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
func (e Exp) Get(key interface{}, attribute string) Exp {
	value := getArgs{table: e, key: Expr(key), attribute: attribute}
	return Exp{kind: getByKeyKind, value: value}
}

// GetById is the same as Get with "id" used as the attribute
//
// Example usage:
//
//  var response map[string]interface{}
//  err := r.Table("heroes").GetById("edc3a46b-95a0-4f64-9d1c-0dd7d83c4bcd").Run(session).One(&response)
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
func (e Exp) GetById(key interface{}) Exp {
	return e.Get(key, "id")
}

type groupByArgs struct {
	attribute        interface{}
	groupedMapReduce GroupedMapReduce
	expr             Exp
}

// GroupBy does a sort of grouped map reduce.  First the server groups all rows
// that have the same value for `attribute`, then it applys the map reduce to
// each group.  It takes a GroupedMapReduce object that specifies how to do the
// map reduce.
//
// `attribute` must be a single attribute (string) or a list of attributes
// ([]string)
//
// The GroupedMapReduce object can be one of the 3 supplied ones: r.Count(),
// r.Avg(attribute), r.Sum(attribute) or a user-supplied object:
//
// Example usage:
//
//  var response []interface{}
//  // Find all heroes with the same durability, calculate their average speed
//  // to see if more durable heroes are slower.
//  err := r.Table("heroes").GroupBy("durability", r.Avg("speed")).Run(session).One(&response)
//
// Example response:
//
//  [
//    {
//      "group": 1,  // this is the strength attribute for every member of this group
//      "reduction": 1.5  // this is the sum of the intelligence attribute of all members of the group
//    },
//    {
//      "group": 2,
//      "reduction": 3.5
//    },
//    ...
//  ]
//
// Example with user-supplied GroupedMapReduce object:
//
//  // Find all heroes with the same strength, sum their intelligence
//  gmr := r.GroupedMapReduce{
//      Mapping: func(row r.Exp) r.Exp { return row.Attr("intelligence") },
//      Base: 0,
//      Reduction: func(acc, val r.Exp) r.Exp { return acc.Add(val) },
//      Finalizer: nil,
//  }
//  err := r.Table("heroes").GroupBy("strength", gmr).Run(session).One(&response)
//
// Example with multiple attributes:
//
//  // Find all heroes with the same strength and speed, sum their intelligence
//  rows := r.Table("heroes").GroupBy([]string{"strength", "speed"}, gmr).Run(session)
func (e Exp) GroupBy(attribute interface{}, groupedMapReduce GroupedMapReduce) Exp {
	return Exp{
		kind: groupByKind,
		value: groupByArgs{
			attribute:        attribute,
			groupedMapReduce: groupedMapReduce,
			expr:             e,
		},
	}
}

type useOutdatedArgs struct {
	expr        Exp
	useOutdated bool
}

// UseOutdated tells the server to use potentially out-of-date data from all
// tables already specified in this query. The advantage is that read queries
// may be faster if this is set.
//
// Example with single table:
//
//  rows := r.Table("heroes").UseOutdated(true).Run(session)
//
// Example with multiple tables (all tables would be allowed to use outdated data):
//
//  villain_strength := r.Table("villains").Get("Doctor Doom", "name").Attr("strength")
//  rows := r.Table("heroes").Filter(r.Row.Attr("strength").Eq(villain_strength)).UseOutdated(true).Run(session)
func (e Exp) UseOutdated(useOutdated bool) Exp {
	value := useOutdatedArgs{expr: e, useOutdated: useOutdated}
	return Exp{kind: useOutdatedKind, value: value}
}

//////////////
// Builtins //
//////////////

type builtinArgs struct {
	operand interface{}
	args    []interface{}
}

func naryBuiltin(kind expressionKind, operand interface{}, args ...interface{}) Exp {
	return Exp{
		kind:  kind,
		value: builtinArgs{operand: operand, args: args},
	}
}

// Attr gets an attribute's value from the row.
//
// Example usage:
//
//  r.Expr(r.Map{"key": "value"}).Attr("key") => "value"
func (e Exp) Attr(name string) Exp {
	return naryBuiltin(getAttributeKind, name, e)
}

// Add sums two numbers or concatenates two arrays.
//
// Example usage:
//
//  r.Expr(1,2,3).Add(r.Expr(4,5,6)) => [1,2,3,4,5,6]
//  r.Expr(2).Add(2) => 4
func (e Exp) Add(operand interface{}) Exp {
	return naryBuiltin(addKind, nil, e, operand)
}

// Sub subtracts two numbers.
//
// Example usage:
//
//  r.Expr(2).Sub(2) => 0
func (e Exp) Sub(operand interface{}) Exp {
	return naryBuiltin(subtractKind, nil, e, operand)
}

// Mul multiplies two numbers.
//
// Example usage:
//
//  r.Expr(2).Mul(3) => 6
func (e Exp) Mul(operand interface{}) Exp {
	return naryBuiltin(multiplyKind, nil, e, operand)
}

// Div divides two numbers.
//
// Example usage:
//
//  r.Expr(3).Div(2) => 1.5
func (e Exp) Div(operand interface{}) Exp {
	return naryBuiltin(divideKind, nil, e, operand)
}

// Mod divides two numbers and returns the remainder.
//
// Example usage:
//
//  r.Expr(23).Mod(10) => 3
func (e Exp) Mod(operand interface{}) Exp {
	return naryBuiltin(moduloKind, nil, e, operand)
}

// And performs a logical and on two values.
//
// Example usage:
//
//  r.Expr(true).And(true) => true
func (e Exp) And(operand interface{}) Exp {
	return naryBuiltin(logicalAndKind, nil, e, operand)
}

// Or performs a logical or on two values.
//
// Example usage:
//
//  r.Expr(true).Or(false) => true
func (e Exp) Or(operand interface{}) Exp {
	return naryBuiltin(logicalOrKind, nil, e, operand)
}

// Eq returns true if two values are equal.
//
// Example usage:
//
//  r.Expr(1).Eq(1) => true
func (e Exp) Eq(operand interface{}) Exp {
	return naryBuiltin(equalityKind, nil, e, operand)
}

// Ne returns true if two values are not equal.
//
// Example usage:
//
//  r.Expr(1).Ne(-1) => true
func (e Exp) Ne(operand interface{}) Exp {
	return naryBuiltin(inequalityKind, nil, e, operand)
}

// Gt returns true if the first value is greater than the second.
//
// Example usage:
//
//  r.Expr(2).Gt(1) => true
func (e Exp) Gt(operand interface{}) Exp {
	return naryBuiltin(greaterThanKind, nil, e, operand)
}

// Gt returns true if the first value is greater than or equal to the second.
//
// Example usage:
//
//  r.Expr(2).Gt(2) => true
func (e Exp) Ge(operand interface{}) Exp {
	return naryBuiltin(greaterThanOrEqualKind, nil, e, operand)
}

// Lt returns true if the first value is less than the second.
//
// Example usage:
//
//  r.Expr(1).Lt(2) => true
func (e Exp) Lt(operand interface{}) Exp {
	return naryBuiltin(lessThanKind, nil, e, operand)
}

// Le returns true if the first value is less than or equal to the second.
//
// Example usage:
//
//  r.Expr(2).Lt(2) => true
func (e Exp) Le(operand interface{}) Exp {
	return naryBuiltin(lessThanOrEqualKind, nil, e, operand)
}

// Not performs a logical not on a value.
//
// Example usage:
//
//  r.Expr(2).Lt(2) => true
func (e Exp) Not() Exp {
	return naryBuiltin(logicalNotKind, nil, e)
}

// ArrayToStream converts an array of objects to a stream.  Many operators work
// on both streams and arrays, but some (such as .Union()) require that both
// operands be the same type.
//
// Example with array (note use of .One()):
//
//  var response []interface{}
//  err := r.Expr(1,2,3).Run(session).One(&response) => [1, 2, 3]
//
// Example with stream (note use of .Collect()):
//
//  var response []interface{}
//  err := r.Expr(1,2,3).ArrayToStream().Run(session).Collect(&response) => [1, 2, 3]
//
// Example with .Union():
//
//  var response []interface{}
//  r.Expr(1,2,3,4).ArrayToStream().Union(r.Table("heroes")).Run(session).Collect(&response)
func (e Exp) ArrayToStream() Exp {
	return naryBuiltin(arrayToStreamKind, nil, e)
}

// StreamToArray converts an stream of objects into an array.  Many operators
// work on both streams and arrays. .Union() requires that both operands be the
// same type.
//
// Example with stream (note use of .Collect()):
//
//  var response []interface{}
//  err := r.Table("heroes").Run(session).Collect(&response) => [{hero...}, {hero...}, ...]
//
// Example with array (note use of .One()):
//
//  var response []interface{}
//  err := r.Table("heroes").StreamToArray().Run(session).One(&response) => [{hero...}, {hero...}, ...]
//
// Example with .Union():
//
//  var response []interface{}
//  err := r.Expr(1,2,3,4).Union(r.Table("heroes").StreamToArray()).Run(session).One(&response)
func (e Exp) StreamToArray() Exp {
	return naryBuiltin(streamToArrayKind, nil, e)
}

// Distinct removes duplicate elements from a sequence.
//
// Example usage:
//
//  var response []interface{}
//  // Get a list of all possible strength values for our heroes
//  err := r.Table("heroes").Map(r.Row.Attr("strength")).Distinct().Run(session).Collect(&response)
//
// Example response:
//
//  [7, 1, 6, 4, 2, 5, 3]
func (e Exp) Distinct() Exp {
	return naryBuiltin(distinctKind, nil, e)
}

// Count returns the number of elements in the response.
//
// Example usage:
//
//  var response int
//  err := r.Table("heroes").Count().Run(session).One(&response)
//
// Example response:
//
//  42
func (e Exp) Count() Exp {
	return naryBuiltin(lengthKind, nil, e)
}

// Merge combines an object with another object, overwriting properties from
// the first with properties from the second.
//
// Example usage:
//
//  var response interface{}
//  firstMap := r.Map{"name": "HAL9000", "role": "Support System"}
//  secondMap := r.Map{"color": "Red", "role": "Betrayal System"}
//  err := r.Expr(firstMap).Merge(secondMap).Run(session).One(&response)
//
// Example response:
//
//  {
//    "color": "Red",
//    "name": "HAL9000",
//    "role": "Betrayal System"
//  }
func (e Exp) Merge(operand interface{}) Exp {
	return naryBuiltin(mapMergeKind, nil, e, operand)
}

// Append appends a value to an array.
//
// Example usage:
//
//  var response []interface{}
//  err := r.Expr(r.List{1, 2, 3, 4}).Append(5).Run(session).One(&response)
//
// Example response:
//
//  [1, 2, 3, 4, 5]
func (e Exp) Append(operand interface{}) Exp {
	return naryBuiltin(arrayAppendKind, nil, e, operand)
}

// Union concatenates two sequences.
//
// Example usage:
//
//  var response []interface{}
//  // Retrieve all heroes and villains
//  r.Table("heroes").Union(r.Table("villains")).Run(session).Collect(&response)
//
// Example response:
//
//  [
//    {
//      "durability": 6,
//      "energy": 6,
//      "fighting": 3,
//      "id": "1a760d0b-57ef-42a8-9fec-c3a1f34930aa",
//      "intelligence": 6,
//      "name": "Iron Man",
//      "real_name": "Anthony Edward \"Tony\" Stark",
//      "speed": 5,
//      "strength": 6
//    },
//    ...
//  ]
func (e Exp) Union(operands ...interface{}) Exp {
	args := []interface{}{e}
	args = append(args, operands...)
	return naryBuiltin(unionKind, nil, args...)
}

// Nth returns the nth element in sequence, zero-indexed.
//
// Example usage:
//
//  var response int
//  // Get the second element of an array
//  err := r.Expr(4,3,2,1).Nth(1).Run(session).One(&response)
//
// Example response:
//
//  3
func (e Exp) Nth(operand interface{}) Exp {
	return naryBuiltin(nthKind, nil, e, operand)
}

// Slice returns a section of a sequence, with bounds [lower, upper), where
// lower bound is inclusive and upper bound is exclusive.
//
// Example usage:
//
//  var response []int
//  err := r.Expr(1,2,3,4,5).Slice(2,4).Run(session).One(&response)
//
// Example response:
//
//  [3, 4]
func (e Exp) Slice(lower, upper interface{}) Exp {
	return naryBuiltin(sliceKind, nil, e, lower, upper)
}

// Limit returns only the first `limit` results from the query.
//
// Example usage:
//
//  var response []int
//  err := r.Expr(1,2,3,4,5).Limit(3).Run(session).One(&response)
//
// Example response:
//
//  [1, 2, 3]
func (e Exp) Limit(limit interface{}) Exp {
	return e.Slice(0, limit)
}

// Skip returns all results after the first `start` results.  Basically it's the
// opposite of .Limit().
//
// Example usage:
//
//  var response []int
//  err := r.Expr(1,2,3,4,5).Skip(3).Run(session).One(&response)
//
// Example response:
//
//  [4, 5]
func (e Exp) Skip(start interface{}) Exp {
	return e.Slice(start, nil)
}

// Map transforms a sequence by applying the given function to each row.
//
// Example usage:
//
//  var squares []int
//  // Square a series of numbers
//  square := func(row r.Exp) r.Exp { return row.Mul(row) }
//  err := r.Expr(1,2,3).Map(square).Run(session).One(&squares)
//
// Example response:
//
//  [1, 2, 3]
//
// Example usage:
//
//  var heroes []interface{}
//  // Fetch multiple rows by primary key
//  heroNames := []string{"Iron Man", "Colossus"}
//  getHero := func (name r.Exp) r.Exp { return r.Table("heroes").Get(name, "name") }
//  err := r.Expr(heroNames).Map(getHero).Run(session).One(&heroes)
//
// Example response:
//
//  [
//    {
//      "durability": 6,
//      "energy": 6,
//      "fighting": 3,
//      "intelligence": 6,
//      "name": "Iron Man",
//      "real_name": "Anthony Edward \"Tony\" Stark",
//      "speed": 5,
//      "strength": 6
//    },
//    ...
//  ]
func (e Exp) Map(operand interface{}) Exp {
	return naryBuiltin(mapKind, operand, e)
}

// ConcatMap constructs a sequence by running the provided function on each row,
// then concatenating all the results.
//
// Example usage:
//
//  var flattened []int
//  // Flatten some nested lists
//  flatten := func(row r.Exp) r.Exp { return row }
//  err := r.Expr(r.List{1,2}, r.List{3,4}).ConcatMap(flatten).Run(session).One(&flattened)
//
// Example response:
//
//  [1, 2, 3, 4]
//
// Example usage:
//
//  var names []string
//  // Get all hero real names and aliases in a list
//  getNames := func(row r.Exp) interface{} { return r.List{row.Attr("name"), row.Attr("real_name")} }
//  err := r.Table("heroes").ConcatMap(getNames).Run(session).Collect(&names)
//
// Example response:
//
//  ["Captain Britain", "Brian Braddock", "Iceman", "Robert \"Bobby\" Louis Drake", ...]
func (e Exp) ConcatMap(operand interface{}) Exp {
	return naryBuiltin(concatMapKind, operand, e)
}

// Filter removes all objects from a sequence that do not match the given
// condition.  The condition can be an RQL expression, an r.Map, or a function
// that returns true or false.
//
// Example with an RQL expression:
//
//   var response []interface{}
//   // Get all heroes with durability 6
//   err := r.Table("heroes").Filter(r.Row.Attr("durability").Eq(6)).Run(session).Collect(&response)
//
// Example with r.Map:
//
//   err := r.Table("heroes").Filter(r.Map{"durability": 6}).Run(session).Collect(&response)
//
// Example with function:
//
//   filterFunc := func (row r.Exp) r.Exp { return row.Attr("durability").Eq(6) }
//   err := r.Table("heroes").Filter(filterFunc).Run(session).Collect(&response)
//
// Example response:
//
//  [
//    {
//      "durability": 6,
//      "energy": 6,
//      "fighting": 3,
//      "id": "1a760d0b-57ef-42a8-9fec-c3a1f34930aa",
//      "intelligence": 6,
//      "name": "Iron Man",
//      "real_name": "Anthony Edward \"Tony\" Stark",
//      "speed": 5,
//      "strength": 6
//    }
//    ...
//  ]
func (e Exp) Filter(operand interface{}) Exp {
	return naryBuiltin(filterKind, operand, e)
}

// Contains returns true if an object has all the given attributes.
//
// Example usage:
//
//  hero := r.Map{"name": "Iron Man", "energy": 6, "speed": 5}
//  r.Expr(hero).Contains("energy", "speed") => true
//  r.Expr(hero).Contains("energy", "guns") => false
func (e Exp) Contains(keys ...string) Exp {
	expr := Expr(true)
	for _, key := range keys {
		expr = expr.And(naryBuiltin(hasAttributeKind, key, e))
	}
	return expr
}

// Pick takes only the given attributes from an object, discarding all other
// attributes. See also .Unpick()
//
// Example usage:
//
//  hero := r.Map{"name": "Iron Man", "energy": 6, "speed": 5}
//  err := r.Expr(hero).Pick("name", "energy").Run(session).One(&response)
//
// Example response:
//
//    {
//      "energy": 6,
//      "name": "Iron Man"
//    }
func (e Exp) Pick(attributes ...string) Exp {
	return naryBuiltin(pickAttributesKind, attributes, e)
}

// Unpick removes the given attributes from an object.  See also .Pick()
//
// Example usage:
//
//  hero := r.Map{"name": "Iron Man", "energy": 6, "speed": 5}
//  err := r.Expr(hero).Unpick("name", "energy").Run(session).One(&response)
//
// Example response:
//
//    {
//      "speed": 5,
//    }
func (e Exp) Unpick(attributes ...string) Exp {
	return naryBuiltin(withoutKind, attributes, e)
}

type rangeArgs struct {
	attribute  string
	lowerbound interface{}
	upperbound interface{}
}

// Between gets all rows where the given primary key attribute's value falls
// between the lowerbound and upperbound (inclusive).
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve all heroes with names between "E" and "F"
//   err := r.Table("heroes").Between("name", "E", "F").Run(session).Collect(&response)
//
// Example response:
//
//  {
//    "strength": 4,
//    "name": "Elektra",
//    "durability": 2,
//    "intelligence": 4,
//    "energy": 3,
//    "fighting": 7,
//    "real_name": "Elektra Natchios",
//    "speed": 6,
//  }
func (e Exp) Between(attribute string, lowerbound, upperbound interface{}) Exp {
	operand := rangeArgs{
		attribute:  attribute,
		lowerbound: lowerbound,
		upperbound: upperbound,
	}

	return naryBuiltin(rangeKind, operand, e)
}

// BetweenIds is the same as .Between() with the primary key attribute name set
// to "id".
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve all heroes with ids between "1" and "2"
//   err := r.Table("heroes").BetweenIds("1", "2").Run(session).Collect(&response)
func (e Exp) BetweenIds(lowerbound, upperbound interface{}) Exp {
	return e.Between("id", lowerbound, upperbound)
}

type orderByArgs struct {
	orderings []interface{}
}

// OrderBy sort the sequence by the values of the given key(s) in each row. The
// default sort is increasing.
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve villains in order of increasing strength
//   err := r.Table("villains").OrderBy("strength").Run(session).Collect(&response)
//
//   // Retrieve villains in order of decreasing strength, then increasing intelligence
//   err := r.Table("villains").OrderBy(r.Desc("strength"), "intelligence").Run(session).Collect(&response)
func (e Exp) OrderBy(orderings ...interface{}) Exp {
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

// Asc tells OrderBy to sort a particular attribute in ascending order.  This is
// the default sort.
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve villains in order of increasing fighting ability (worst fighters first)
//   err := r.Table("villains").OrderBy(r.Asc("fighting")).Run(session).Collect(&response)
func Asc(attr string) orderByAttr {
	return orderByAttr{attr, true}
}

// Desc tells OrderBy to sort a particular attribute in descending order.
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve villains in order of decreasing speed (fastest villains first)
//   err := r.Table("villains").OrderBy(r.Desc("speed")).Run(session).Collect(&response)
func Desc(attr string) orderByAttr {
	return orderByAttr{attr, false}
}

type reduceArgs struct {
	base      interface{}
	reduction interface{}
}

// Reduce iterates over a sequence, starting with a base value and applying a
// reduction function to the value so far and the next row of the sequence.
//
// Currently, the reduction function must satisfy the constraint:
//
//  reduction(base, value) == value
//
// This restriction may be removed in a future version of RethinkDB.
//
// Example usage:
//
//  var sum int
//  // Add the numbers 1-4 together
//  reduction := func(acc, val r.Exp) r.Exp { return acc.Add(val) }
//  err := r.Expr(1,2,3,4).Reduce(0, reduction).Run(session).One(&sum)
//
// Example response:
//
//  10
//
// Example usage:
//
//  var totalSpeed int
//  // Compute the total speed for all heroes, the types of acc and val should
//  // be the same, so we extract the speed first with a .Map()
//  mapping := func(row r.Exp) r.Exp { return row.Attr("speed") }
//  reduction := func(acc, val r.Exp) r.Exp { return acc.Add(val) }
//  err := r.Table("heroes").Map(mapping).Reduce(0, reduction).Run(session).One(&totalSpeed)
//
// Example response:
//
//  232
func (e Exp) Reduce(base, reduction interface{}) Exp {
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

// GroupedMapReduce partitions a sequence into groups, then performs a map and a
// reduction on each group.  See also .Map() and .GroupBy().
//
// Example usage:
//
//  // Find the sum of the even and odd numbers separately
//  grouping := func(row r.Exp) r.Exp { return r.Branch(row.Mod(2).Eq(0), "even", "odd") }
//  mapping := func(row r.Exp) r.Exp { return row }
//  base := 0
//  reduction := func(acc, row r.Exp) r.Exp {
//  	return acc.Add(row)
//  }
//
//  var response []interface{}
//  err := r.Expr(1,2,3,4,5).GroupedMapReduce(grouping, mapping, base, reduction).Run(session).One(&response)
//
// Example response:
//
//  [
//    {
//      "group": "even",
//      "reduction": 6
//    },
//    {
//      "group": "odd",
//      "reduction": 9
//    }
//  ]
//
// Example usage:
//
//  // Group all heroes by intelligence, then find the most fastest one in each group
//  grouping := func(row r.Exp) r.Exp { return row.Attr("intelligence") }
//  mapping := func(row r.Exp) r.Exp { return row.Pick("name", "speed") }
//  base := r.Map{"name": nil, "speed": 0}
//  reduction := func(acc, row r.Exp) r.Exp {
//  	return r.Branch(acc.Attr("speed").Lt(row.Attr("speed")), row, acc)
//  }
//
//  var response []interface{}
//  err := r.Table("heroes").GroupedMapReduce(grouping, mapping, base, reduction).Run(session).One(&response)
//
// Example response:
//
//  [
//    {
//      "group": 1,
//      "reduction": {
//        "name": "Northstar",
//        "speed": 2
//      }
//    },
//    {
//      "group": 2,
//      "reduction": {
//        "name": "Thor",
//        "speed": 6
//      }
//    },
//    ...
//  ]
func (e Exp) GroupedMapReduce(grouping, mapping, base, reduction interface{}) Exp {
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

// Pluck runs .Pick() for each row in the sequence, removing all but the
// specified attributes from each row. See also .Without().
//
// Example usage:
//
//  var heroes []interface{}
//  err := r.Table("heroes").Pluck("real_name", "id").Run(session).Collect(&heroes)
//
// Example response:
//
//  [
//    {
//      "real_name": "Peter Benjamin Parker",
//      "id": "1227f639-38f0-4cbb-a7b1-9c49f13fe89d",
//    },
//    ...
//  ]
func (e Exp) Pluck(attributes ...string) Exp {
	return e.Map(Row.Pick(attributes...))
}

// Without runs .Unpick() for each row in the sequence, removing any specified
// attributes from each individual row.  See also .Pluck().
//
// Example usage:
//
//  var heroes []interface{}
//  err := r.Table("heroes").Without("real_name", "id").Run(session).Collect(&heroes)
//
// Example response:
//
//  [
//    {
//      "durability": 4,
//      "energy": 7,
//      "fighting": 4,
//      "intelligence": 7,
//      "name": "Professor X",
//      "speed": 2,
//      "strength": 4
//    },
//    ...
//  ]
func (e Exp) Without(attributes ...string) Exp {
	return e.Map(Row.Unpick(attributes...))
}

// InnerJoin performs an inner join on two sequences, using the provided
// function to compare the rows from each sequence. See also .EqJoin() and
// .OuterJoin().
//
// Each row from the left sequence is compared to every row from the right
// sequence using the provided predicate function.  If the function returns
// true for a pair of rows, that pair will appear in the resulting sequence.
//
// Example usage:
//
//  var response []interface{}
//  // Get each hero and their associated lair, in this case, "villain_id" is
//  // the primary key for the "lairs" table
//  compareRows := func (left, right r.Exp) r.Exp { return left.Attr("id").Eq(right.Attr("villain_id")) }
//  err := r.Table("villains").InnerJoin(r.Table("lairs"), compareRows).Run(session).Collect(&response)
//
// Example response:
//
//  [
//    {
//      "left": {
//        "durability": 6,
//        "energy": 6,
//        "fighting": 3,
//        "id": "c0d1b94f-b07e-40c3-a1db-448e645daedc",
//        "intelligence": 6,
//        "name": "Magneto",
//        "real_name": "Max Eisenhardt",
//        "speed": 4,
//        "strength": 2
//      },
//      "right": {
//        "lair": "Asteroid M",
//        "villain_id": "c0d1b94f-b07e-40c3-a1db-448e645daedc"
//      }
//    }
//  ]
func (leftExpr Exp) InnerJoin(rightExpr Exp, predicate func(Exp, Exp) Exp) Exp {
	return leftExpr.ConcatMap(func(left Exp) interface{} {
		return rightExpr.ConcatMap(func(right Exp) interface{} {
			return Branch(predicate(left, right),
				List{Map{"left": left, "right": right}},
				List{},
			)
		})
	})
}

// OuterJoin performs a left outer join on two sequences, using the provided
// function to compare the rows from each sequence. See also .EqJoin() and
// .InnerJoin().
//
// Each row from the left sequence is compared to every row from the right
// sequence using the provided predicate function.  If the function returns
// true for a pair of rows, that pair will appear in the resulting sequence.
//
// If the predicate is false for every pairing for a specific left row, the left
// row will appear in the sequence with no right row present.
//
// Example usage:
//
//  var response []interface{}
//  // Get each hero and their associated lair, in this case, "villain_id" is
//  // the primary key for the "lairs" table
//  compareRows := func (left, right r.Exp) r.Exp { return left.Attr("id").Eq(right.Attr("villain_id")) }
//  err := r.Table("villains").OuterJoin(r.Table("lairs"), compareRows).Run(session).Collect(&response)
//
// Example response:
//
//  [
//    {
//      "left": {
//        "durability": 6,
//        "energy": 6,
//        "fighting": 3,
//        "id": "c0d1b94f-b07e-40c3-a1db-448e645daedc",
//        "intelligence": 6,
//        "name": "Magneto",
//        "real_name": "Max Eisenhardt",
//        "speed": 4,
//        "strength": 2
//      },
//      "right": {
//        "lair": "Asteroid M",
//        "villain_id": "c0d1b94f-b07e-40c3-a1db-448e645daedc"
//      }
//    },
//    {
//      "left": {
//        "durability": 4,
//        "energy": 1,
//        "fighting": 7,
//        "id": "ab140a9c-63d1-455e-862e-045ad7f57ae3",
//        "intelligence": 2,
//        "name": "Sabretooth",
//        "real_name": "Victor Creed",
//        "speed": 2,
//        "strength": 4
//      }
//    }
//    ...
//  ]
func (leftExpr Exp) OuterJoin(rightExpr Exp, predicate func(Exp, Exp) Exp) Exp {
	return leftExpr.ConcatMap(func(left Exp) interface{} {
		return Let(Map{"matches": rightExpr.ConcatMap(func(right Exp) Exp {
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

// EqJoin performs a join on two expressions, it is more efficient than
// .InnerJoin() and .OuterJoin() because it looks up elements in the right table
// by primary key. See also .InnerJoin() and .OuterJoin().
//
// Example usage:
//
//  var response []interface{}
//  // Get each hero and their associated lair, in this case, "villain_id" is
//  // the primary key for the "lairs" table
//  err := r.Table("villains").EqJoin("id", r.Table("lairs"), "villain_id").Run(session).Collect(&response)
//
// Example response:
//
//  [
//    {
//      "left": {
//        "durability": 6,
//        "energy": 6,
//        "fighting": 3,
//        "id": "c0d1b94f-b07e-40c3-a1db-448e645daedc",
//        "intelligence": 6,
//        "name": "Magneto",
//        "real_name": "Max Eisenhardt",
//        "speed": 4,
//        "strength": 2
//      },
//      "right": {
//        "lair": "Asteroid M",
//        "villain_id": "c0d1b94f-b07e-40c3-a1db-448e645daedc"
//      }
//    },
//    ...
//  ]
func (leftExpr Exp) EqJoin(leftAttribute string, rightExpr Exp, rightAttribute string) Exp {
	return leftExpr.ConcatMap(func(left Exp) interface{} {
		return Let(Map{"right": rightExpr.Get(left.Attr(leftAttribute), rightAttribute)},
			Branch(LetVar("right").Ne(nil),
				List{Map{"left": left, "right": LetVar("right")}},
				List{},
			))
	})
}

// Zip flattens the results of a join by merging the "left" and "right" fields
// of each row together.  If any keys conflict, the "right" field takes
// precedence.
//
// Example without .Zip():
//
//  var response []interface{}
//  // Find each hero-villain pair with the same strength
//  equalStrength := func(hero, villain r.Exp) r.Exp {
//      return hero.Attr("strength").Eq(villain.Attr("strength"))
//  }
//  err := r.Table("heroes").InnerJoin(r.Table("villains"), equalStrength).Run(session).Collect(&response)
//
// Example response:
//
//  [
//    {
//      "left":
//      {
//        "durability": 5,
//        "energy": 7,
//        "fighting": 7,
//        "id": "f915d9a7-6cfa-4151-b5f6-6aded7da595f",
//        "intelligence": 5,
//        "name": "Nightcrawler",
//        "real_name": "Kurt Wagner",
//        "speed": 7,
//        "strength": 4
//      },
//      "right":
//      {
//        "durability": 4,
//        "energy": 1,
//        "fighting": 7,
//        "id": "12e58b11-93b3-4e89-987d-efb345001dfe",
//        "intelligence": 2,
//        "name": "Sabretooth",
//        "real_name": "Victor Creed",
//        "speed": 2,
//        "strength": 4
//      }
//    },
//    ...
//  ]
//
// Example with .Zip():
//
//  var response []interface{}
//  // Find each hero-villain pair with the same strength
//  equalStrength := func(hero, villain r.Exp) r.Exp {
//      return hero.Attr("strength").Eq(villain.Attr("strength"))
//  }
//  err := r.Table("heroes").InnerJoin(r.Table("villains"), equalStrength).Zip().Run(session).Collect(&response)
//
// Example response:
//
//  [
//    {
//      "durability": 4,
//      "energy": 1,
//      "fighting": 7,
//      "id": "12e58b11-93b3-4e89-987d-efb345001dfe",
//      "intelligence": 2,
//      "name": "Sabretooth",
//      "real_name": "Victor Creed",
//      "speed": 2,
//      "strength": 4
//    },
//    ...
//  ]
func (e Exp) Zip() Exp {
	return e.Map(func(row Exp) interface{} {
		return Branch(
			row.Contains("right"),
			row.Attr("left").Merge(row.Attr("right")),
			row.Attr("left"),
		)
	})
}

// GroupedMapReduce stores all the expressions needed to perform a .GroupBy()
// call, there are three pre-made ones: r.Count(), r.Sum(attribute), and
// r.Avg(attribute).  See the documentation for .GroupBy() for more information.
type GroupedMapReduce struct {
	Mapping   interface{}
	Base      interface{}
	Reduction interface{}
	Finalizer interface{}
}

// Count counts the number of rows in a group, for use with the .GroupBy()
// method.
//
// Example usage:
//
//  var response []interface{}
//  // Count all heroes in each superhero group
//  err := r.Table("heroes").GroupBy("affiliation", r.Count()).Run(session).One(&response)
//
// Example response:
//
//  [
//    {
//      "group": "Avengers", // this is the affiliation attribute for every member of this group
//      "reduction": 9  // this is the number of members in this group
//    },
//    {
//      "group": "X-Men",
//      "reduction": 12
//    },
//    ...
//  ]
func Count() GroupedMapReduce {
	return GroupedMapReduce{
		Mapping:   func(row Exp) interface{} { return 1 },
		Base:      0,
		Reduction: func(acc, val Exp) interface{} { return acc.Add(val) },
	}
}

// Sum computes the sum of an attribute for a group, for use with the .GroupBy()
// method.
//
// Example usage:
//
//  var response []interface{}
//  // Get the total intelligence of all heroes who have the same strength
//  err := r.Table("heroes").GroupBy("strength", r.Sum("intelligence")).Run(session).One(&response)
//
// Example response:
//
//  [
//    {
//      // this is the strength attribute for every member of this group
//      "group": 1,
//      // this is the sum of the intelligence attribute of all members of the group
//      "reduction": 2
//    },
//    {
//      "group": 2,
//      "reduction": 15
//    },
//    ...
//  ]
func Sum(attribute string) GroupedMapReduce {
	return GroupedMapReduce{
		Mapping:   func(row Exp) interface{} { return row.Attr(attribute) },
		Base:      0,
		Reduction: func(acc, val Exp) interface{} { return acc.Add(val) },
	}
}

// Avg computes the average value of an attribute for a group, for use with the
// .GroupBy() method.
//
// Example usage:
//
//  var response []interface{}
//  err := r.Table("heroes").GroupBy("strength", r.Avg("intelligence")).Run(session).One(&response)
//
// Example response:
//
//  [
//    {
//      "group": 1, // this is the strength attribute for every member of this group
//      "reduction": 1  // this is the average value of the intelligence attribute of all members of the group
//    },
//    {
//      "group": 2,
//      "reduction": 3
//    },
//    ...
//  ]
func Avg(attribute string) GroupedMapReduce {
	return GroupedMapReduce{
		Mapping: func(row Exp) interface{} {
			return List{row.Attr(attribute), 1}
		},
		Base: []int{0, 0},
		Reduction: func(acc, val Exp) interface{} {
			return []interface{}{
				acc.Nth(0).Add(val.Nth(0)),
				acc.Nth(1).Add(val.Nth(1)),
			}
		},
		Finalizer: func(row Exp) interface{} {
			return row.Nth(0).Div(row.Nth(1))
		},
	}
}

// Meta Queries
// Database administration (e.g. database create, table drop, etc)

type createDatabaseQuery struct {
	name string
}

// DbCreate creates a database with the supplied name.
//
// Example usage:
//
//  err := r.DbCreate("marvel").Run(session).Exec()
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
//  err := r.DbDrop("marvel").Run(session).Exec()
func DbDrop(name string) MetaQuery {
	return MetaQuery{query: dropDatabaseQuery{name}}
}

type listDatabasesQuery struct{}

// DbList lists all databases on the server
//
// Example usage:
//
//  var databases []string
//  err := r.DbList().Run(session).Collect(&databases)
//
// Example response:
//
//  ["test", "marvel"]
func DbList() MetaQuery {
	return MetaQuery{query: listDatabasesQuery{}}
}

type database struct {
	name string
}

// Db lets you perform operations within a specific database (this will override
// the database specified on the session).  This can be used to access or
// create/list/delete tables within any database available on the server.
//
// Example usage:
//
//  var response []interface{}
//  // this query will use the default database of the last created session
//  r.Table("test").Run(session).Collect(&response)
//  // this query will use database "marvel" regardless of what database the session has set
//  r.Db("marvel").Table("heroes").Run(session).Collect(&response)
func Db(name string) database {
	return database{name}
}

type tableCreateQuery struct {
	database database
	spec     TableSpec
}

// TableSpec lets you specify the various parameters for a table, then create it
// with TableCreateSpec().  See that function for documentation.
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
//  err := r.TableCreate("heroes").Run(session).Exec()
func TableCreate(name string) MetaQuery {
	spec := TableSpec{Name: name}
	return MetaQuery{query: tableCreateQuery{spec: spec}}
}

func (db database) TableCreate(name string) MetaQuery {
	spec := TableSpec{Name: name}
	return MetaQuery{query: tableCreateQuery{spec: spec, database: db}}
}

// TableCreateSpec creates a table with the specified attributes.
//
// Example usage:
//
//  spec := TableSpec{Name: "heroes", PrimaryKey: "name"}
//  err := r.TableCreateSpec(spec).Run(session).Exec()
func TableCreateSpec(spec TableSpec) MetaQuery {
	return MetaQuery{query: tableCreateQuery{spec: spec}}
}

func (db database) TableCreateSpec(spec TableSpec) MetaQuery {
	return MetaQuery{query: tableCreateQuery{spec: spec, database: db}}
}

type tableListQuery struct {
	database database
}

// TableList lists all tables in the database.
//
// Example usage:
//
//  var tables []string
//  err := r.TableList().Run(session).Collect(&tables)
//
// Example response:
//
//  ["heroes", "villains"]
func TableList() MetaQuery {
	return MetaQuery{query: tableListQuery{}}
}

func (db database) TableList() MetaQuery {
	return MetaQuery{query: tableListQuery{db}}
}

type tableDropQuery struct {
	table tableInfo
}

// TableDrop removes a table from the database.
//
// Example usage:
//
//  err := r.Db("marvel").TableDrop("heroes").Run(session).Exec()
func TableDrop(name string) MetaQuery {
	table := tableInfo{name: name}
	return MetaQuery{query: tableDropQuery{table: table}}
}

func (db database) TableDrop(name string) MetaQuery {
	table := tableInfo{name: name, database: db}
	return MetaQuery{query: tableDropQuery{table: table}}
}

type tableInfo struct {
	name     string
	database database
}

// Table references all rows in a specific table, using the database that this
// method was called on.
//
// Example usage:
//
//  var response []map[string]interface{}
//  err := r.Db("marvel").Table("heroes").Run(session).Collect(&response)
//
// Example response:
//
//  [
//    {
//      "strength": 3,
//      "name": "Doctor Strange",
//      "durability": 6,
//      "intelligence": 4,
//      "energy": 7,
//      "fighting": 7,
//      "real_name": "Stephen Vincent Strange",
//      "speed": 5,
//      "id": "edc3a46b-95a0-4f64-9d1c-0dd7d83c4bcd"
//    },
//    ...
//  ]
func Table(name string) Exp {
	value := tableInfo{name: name}
	return Exp{kind: tableKind, value: value}
}

func (db database) Table(name string) Exp {
	value := tableInfo{name: name, database: db}
	return Exp{kind: tableKind, value: value}
}

type insertQuery struct {
	tableExpr Exp
	rows      []interface{}
}

// Insert inserts rows into the database.  If no value is specified for the
// primary key (by default "id"), a value will be generated by the server, e.g.
// "05679c96-9a05-4f42-a2f6-a9e47c45a5ae".
//
// Example usage:
//
//  var response r.WriteResponse
//  row := r.Map{"name": "Thing"}
//  err := r.Table("heroes").Insert(row).Run(session).One(&response)
func (e Exp) Insert(rows ...interface{}) WriteQuery {
	// Assume the expression is a table for now, we'll check later in buildProtobuf
	return WriteQuery{query: insertQuery{
		tableExpr: e,
		rows:      rows,
	}}
}

// Overwrite tells an Insert query to overwrite existing rows instead of
// returning an error.
//
// Example usage:
//
//  var response r.WriteResponse
//  row := r.Map{"name": "Thing"}
//  err := r.Table("heroes").Insert(row).Overwrite(true).Run(session).One(&response)
func (q WriteQuery) Overwrite(overwrite bool) WriteQuery {
	q.overwrite = overwrite
	return q
}

// Atomic changes the required atomic-ness of a query.  By default queries will
// only be run if they can be executed atomically, that is, all at once.  If a
// query may not be executed atomically, the server will return an error.  To
// disable the atomic requirement, use .Atomic(false).
//
// Example usage:
//
//  var response r.WriteResponse
//  id := "05679c96-9a05-4f42-a2f6-a9e47c45a5ae"
//  replacement := r.Map{"name": r.Js("Thing")}
//  // The following will return an error, because of the use of r.Js
//  err := r.Table("heroes").GetById(id).Update(replacement).Run(session).One(&response)
//  // This will work
//  err := r.Table("heroes").GetById(id).Update(replacement).Atomic(false).Run(session).One(&response)
func (q WriteQuery) Atomic(atomic bool) WriteQuery {
	q.nonatomic = !atomic
	return q
}

type updateQuery struct {
	view    Exp
	mapping interface{}
}

// Update updates rows in the database. Accepts a JSON document, a RQL
// expression, or a combination of the two.
//
// Example usage:
//
//  var response r.WriteResponse
//  replacement := r.Map{"name": "Thing"}
//  // Update a single row by id
//  id := "05679c96-9a05-4f42-a2f6-a9e47c45a5ae"
//  err := r.Table("heroes").GetById(id).Update(replacement).Run(session).One(&response)
//  // Update all rows in the database
//  err := r.Table("heroes").Update(replacement).Run(session).One(&response)
func (e Exp) Update(mapping interface{}) WriteQuery {
	return WriteQuery{query: updateQuery{
		view:    e,
		mapping: mapping,
	}}
}

type replaceQuery struct {
	view    Exp
	mapping interface{}
}

// Replace replaces rows in the database. Accepts a JSON document or a RQL
// expression, and replaces the original document with the new one. The new
// row must have the same primary key as the original document.
//
// Example usage:
//
//  var response r.WriteResponse
//
//  // Replace a single row by id
//  id := "05679c96-9a05-4f42-a2f6-a9e47c45a5ae"
//  replacement := r.Map{"id": r.Row.Attr("id"), "name": "Thing"}
//  err := r.Table("heroes").GetById(id).Replace(replacement).Run(session).One(&response)
//
//  // Replace all rows in a table
//  err := r.Table("heroes").Replace(replacement).Run(session).One(&response)
func (e Exp) Replace(mapping interface{}) WriteQuery {
	return WriteQuery{query: replaceQuery{
		view:    e,
		mapping: mapping,
	}}
}

type deleteQuery struct {
	view Exp
}

// Delete removes one or more rows from the database.
//
// Example usage:
//
//  var response r.WriteResponse
//
//  // Delete a single row by id
//  err := r.Table("heroes").GetById("5d93edbb-2882-4594-8163-f64d8695e575").Delete().Run(session).One(&response)
//
//  // Delete all rows in a table
//  err := r.Table("heroes").Delete().Run(session).One(&response)
//
//  // Find a row, then delete it
//  err := r.Table("heroes").Filter(r.Map{"real_name": "Peter Benjamin Parker"}).Delete().Run(session).One(&response)
func (e Exp) Delete() WriteQuery {
	return WriteQuery{query: deleteQuery{view: e}}
}

type forEachQuery struct {
	stream    Exp
	queryFunc func(Exp) Query
}

// ForEach runs a given write query for each row of a sequence.
//
// Example usage:
//
//  // Delete all rows with the given ids
//
//  var response r.WriteResponse
//  // Delete multiple rows by primary key
//  heroNames := []string{"Iron Man", "Colossus"}
//  deleteHero := func (name r.Exp) r.Query { return r.Table("heroes").Get(name, "name").Delete() }
//  err := r.Expr(heroNames).ForEach(deleteHero).Run(session).One(&response)
//
// Example response:
//
//  {
//    "deleted": 2
//  }
func (e Exp) ForEach(queryFunc (func(Exp) Query)) WriteQuery {
	return WriteQuery{query: forEachQuery{stream: e, queryFunc: queryFunc}}
}
