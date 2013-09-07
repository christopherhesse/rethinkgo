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
	addKind expressionKind = iota
	allKind
	anyKind
	appendKind
	ascendingKind
	betweenKind
	branchKind
	changeAtKind
	coerceToKind
	concatMapKind
	containsKind
	countKind
	databaseCreateKind
	databaseDropKind
	databaseKind
	databaseListKind
	deleteAtKind
	deleteKind
	descendingKind
	differenceKind
	distinctKind
	divideKind
	defaultKind
	eqJoinKind
	equalityKind
	errorKind
	filterKind
	forEachKind
	funcallKind
	funcKind
	getAllKind
	getFieldKind
	getKind
	greaterThanKind
	greaterThanOrEqualKind
	groupByKind
	groupedMapReduceKind
	hasFieldsKind
	implicitVariableKind
	indexCreateKind
	indexDropKind
	indexesOfKind
	indexListKind
	inequalityKind
	infoKind
	innerJoinKind
	insertAtKind
	insertKind
	isEmptyKind
	javascriptKind
	jsonKind
	keysKind
	lessThanKind
	lessThanOrEqualKind
	limitKind
	logicalNotKind
	mapKind
	matchKind
	mergeKind
	moduloKind
	multiplyKind
	nthKind
	orderByKind
	outerJoinKind
	pluckKind
	prependKind
	reduceKind
	returnValuesKind
	replaceKind
	sampleKind
	setDifferenceKind
	setInsertKind
	setIntersectionKind
	setUnionKind
	skipKind
	sliceKind
	spliceAtKind
	subtractKind
	tableCreateKind
	tableDropKind
	tableKind
	tableListKind
	typeOfKind
	unionKind
	updateKind
	variableKind
	withFieldsKind
	withoutKind
	zipKind

	// custom rethinkgo ones
	upsertKind
	atomicKind
	useOutdatedKind
	durabilityKind
	literalKind
	leftboundKind
	rightboundKind
)

func nullaryOperator(kind expressionKind) Exp {
	return Exp{kind: kind}
}

func naryOperator(kind expressionKind, operand interface{}, operands ...interface{}) Exp {
	args := []interface{}{operand}
	args = append(args, operands...)
	return Exp{kind: kind, args: args}
}

func stringsToInterfaces(strings []string) []interface{} {
	interfaces := make([]interface{}, len(strings))
	for i, v := range strings {
		interfaces[i] = interface{}(v)
	}
	return interfaces
}

func funcWrapper(f interface{}, arity int) Exp {
	return naryOperator(funcKind, f, arity)
}

// Exp represents an RQL expression, such as the return value of
// r.Expr(). Exp has all the RQL methods on it, such as .Add(), .Attr(),
// .Filter() etc.
//
// To create an Exp from a native or user-defined type, or function, use
// r.Expr().
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
//  err := r.Table("heroes").Map(getIntelligence).Run(session).All(&response)
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
	args []interface{}
	kind expressionKind
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
//  }).Run(session).All(&response)
//
// Example with Row:
//
//  var response []interface{}
//  // Get the real names of all the villains
//  err := r.Table("employees").Map(Row.Attr("real_name")).Run(session).All(&response)
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
func Expr(value interface{}) Exp {
	v, ok := value.(Exp) // check if it's already an Exp
	if ok {
		return v
	}
	return naryOperator(literalKind, value)
}

// Json creates an object using a literal json string.
//
// Example usage:
//
//  var response interface{}
//  rows := r.Json(`"go": "awesome", "rethinkdb": "awesomer"}`).Run(session).One(&response)
//
// Example response:
//
//  {"go": "awesome", "rethinkdb": "awesomer"}
func Json(value string) Exp {
	return naryOperator(jsonKind, value)
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
// JsWithTimeout lets you specify a timeout for the javascript expression.
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
//  err := r.Table("heroes").Map(func(row r.Exp) r.Exp {
//      return row.Attr("strength").Add(row.Attr("durability"))
//  }).Run(session).All(&response)
//
// Example with Js:
//
//  var response []interface{}
//  // Combine each hero's strength and durability
//  err := r.Table("heroes").Map(
//  	r.Js(`(function (row) { return row.strength + row.durability; })`),
//  ).Run(session).All(&response)
//
// Example response:
//
//  [11, 6, 9, 11, ...]
func Js(body string) Exp {
	return naryOperator(javascriptKind, body)
}

// JsWithTimeout lets you specify the timeout for a javascript expression to run
// (in seconds). The default value is 5 seconds.
func JsWithTimeout(body string, timeout float64) Exp {
	return naryOperator(javascriptKind, body, timeout)
}

// RuntimeError tells the server to respond with a ErrRuntime, useful for
// testing.
//
// Example usage:
//
//  err := r.RuntimeError("hi there").Run(session).Err()
func RuntimeError(message string) Exp {
	return Exp{kind: errorKind, args: List{message}}
}

// Branch checks a test expression, evaluating the trueBranch expression if it's
// true and falseBranch otherwise.
//
// Example usage:
//
//  // Roughly equivalent RQL expression
//  r.Branch(r.Row.Attr("first_name").Eq("Marc"), "is probably marc", "who cares")
func Branch(test, trueBranch, falseBranch interface{}) Exp {
	return naryOperator(branchKind, test, trueBranch, falseBranch)
}

// Get retrieves a single row by primary key.
//
// Example usage:
//
//  var response map[string]interface{}
//  err := r.Table("heroes").Get("Doctor Strange").Run(session).One(&response)
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
func (e Exp) Get(key interface{}) Exp {
	return naryOperator(getKind, e, key)
}

// GetAll retrieves all documents where the given value matches the requested
// index.
//
// Example usage (awesomeness is a secondary index defined as speed * strength):
//
//  var response []interface{}
//  err := r.Table("heroes").GetAll("awesomeness", 10).Run(session).All(&response)
//
// Example response:
//
//  {
//    "strength": 2,
//    "name": "Storm",
//    "durability": 3,
//    "intelligence": 5,
//    "energy": 6,
//    "fighting": 5,
//    "real_name": "Ororo Munroe",
//    "speed": 5,
//    "id": "59d1ad55-a61e-49d9-a375-0fb014b0e6ea"
//  }
func (e Exp) GetAll(index string, values ...interface{}) Exp {
	return naryOperator(getAllKind, e, append(values, index)...)
}

// GroupBy does a sort of grouped map reduce.  First the server groups all rows
// that have the same value for `attribute`, then it applys the map reduce to
// each group.  It takes one of the following reductions: r.Count(),
// r.Sum(string), r.Avg(string)
//
// `attribute` must be a single attribute (string) or a list of attributes
// ([]string)
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
// Example with multiple attributes:
//
//  // Find all heroes with the same strength and speed, sum their intelligence
//  rows := r.Table("heroes").GroupBy([]string{"strength", "speed"}, r.Count()).Run(session)
func (e Exp) GroupBy(attribute, groupedMapReduce interface{}) Exp {
	_, ok := attribute.(string)
	if ok {
		attribute = List{attribute}
	}
	return naryOperator(groupByKind, e, attribute, groupedMapReduce)
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
//  compareFunc := r.Row.Attr("strength").Eq(villain_strength)
//  rows := r.Table("heroes").Filter(compareFunc).UseOutdated(true).Run(session)
func (e Exp) UseOutdated(useOutdated bool) Exp {
	return naryOperator(useOutdatedKind, e, useOutdated)
}

// Durability sets the durability for the expression, this can be set to either
// "soft" or "hard".
//
// Example usage:
//
//  var response r.WriteResponse
//  r.Table("heroes").Insert(r.Map{"superhero": "Iron Man"}).Durability("soft").Run(session).One(&response)
//
// Example response:
func (e Exp) Durability(durability string) Exp {
	return naryOperator(durabilityKind, e, durability)
}

// Attr gets an attribute's value from the row.
//
// Example usage:
//
//  r.Expr(r.Map{"key": "value"}).Attr("key") => "value"
func (e Exp) Attr(name string) Exp {
	return naryOperator(getFieldKind, e, name)
}

// Add sums two numbers or concatenates two arrays.
//
// Example usage:
//
//  r.Expr(1,2,3).Add(r.Expr(4,5,6)) => [1,2,3,4,5,6]
//  r.Expr(2).Add(2) => 4
func (e Exp) Add(operand interface{}) Exp {
	return naryOperator(addKind, e, operand)
}

// Sub subtracts two numbers.
//
// Example usage:
//
//  r.Expr(2).Sub(2) => 0
func (e Exp) Sub(operand interface{}) Exp {
	return naryOperator(subtractKind, e, operand)
}

// Mul multiplies two numbers.
//
// Example usage:
//
//  r.Expr(2).Mul(3) => 6
func (e Exp) Mul(operand interface{}) Exp {
	return naryOperator(multiplyKind, e, operand)
}

// Div divides two numbers.
//
// Example usage:
//
//  r.Expr(3).Div(2) => 1.5
func (e Exp) Div(operand interface{}) Exp {
	return naryOperator(divideKind, e, operand)
}

// Mod divides two numbers and returns the remainder.
//
// Example usage:
//
//  r.Expr(23).Mod(10) => 3
func (e Exp) Mod(operand interface{}) Exp {
	return naryOperator(moduloKind, e, operand)
}

// And performs a logical and on two values.
//
// Example usage:
//
//  r.Expr(true).And(true) => true
func (e Exp) And(operand interface{}) Exp {
	return naryOperator(allKind, e, operand)
}

// Or performs a logical or on two values.
//
// Example usage:
//
//  r.Expr(true).Or(false) => true
func (e Exp) Or(operand interface{}) Exp {
	return naryOperator(anyKind, e, operand)
}

// Eq returns true if two values are equal.
//
// Example usage:
//
//  r.Expr(1).Eq(1) => true
func (e Exp) Eq(operand interface{}) Exp {
	return naryOperator(equalityKind, e, operand)
}

// Ne returns true if two values are not equal.
//
// Example usage:
//
//  r.Expr(1).Ne(-1) => true
func (e Exp) Ne(operand interface{}) Exp {
	return naryOperator(inequalityKind, e, operand)
}

// Gt returns true if the first value is greater than the second.
//
// Example usage:
//
//  r.Expr(2).Gt(1) => true
func (e Exp) Gt(operand interface{}) Exp {
	return naryOperator(greaterThanKind, e, operand)
}

// Gt returns true if the first value is greater than or equal to the second.
//
// Example usage:
//
//  r.Expr(2).Gt(2) => true
func (e Exp) Ge(operand interface{}) Exp {
	return naryOperator(greaterThanOrEqualKind, e, operand)
}

// Lt returns true if the first value is less than the second.
//
// Example usage:
//
//  r.Expr(1).Lt(2) => true
func (e Exp) Lt(operand interface{}) Exp {
	return naryOperator(lessThanKind, e, operand)
}

// Le returns true if the first value is less than or equal to the second.
//
// Example usage:
//
//  r.Expr(2).Lt(2) => true
func (e Exp) Le(operand interface{}) Exp {
	return naryOperator(lessThanOrEqualKind, e, operand)
}

// Not performs a logical not on a value.
//
// Example usage:
//
//  r.Expr(true).Not() => false
func (e Exp) Not() Exp {
	return naryOperator(logicalNotKind, e)
}

// Distinct removes duplicate elements from a sequence.
//
// Example usage:
//
//  var response []interface{}
//  // Get a list of all possible strength values for our heroes
//  err := r.Table("heroes").Map(r.Row.Attr("strength")).Distinct().Run(session).All(&response)
//
// Example response:
//
//  [7, 1, 6, 4, 2, 5, 3]
func (e Exp) Distinct() Exp {
	return naryOperator(distinctKind, e)
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
	return naryOperator(countKind, e)
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
	return naryOperator(mergeKind, e, operand)
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
func (e Exp) Append(value interface{}) Exp {
	return naryOperator(appendKind, e, value)
}

// Union concatenates two sequences.
//
// Example usage:
//
//  var response []interface{}
//  // Retrieve all heroes and villains
//  r.Table("heroes").Union(r.Table("villains")).Run(session).All(&response)
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
	return naryOperator(unionKind, e, operands...)
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
	return naryOperator(nthKind, e, operand)
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
	return naryOperator(sliceKind, e, lower, upper)
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
	return naryOperator(limitKind, e, limit)
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
	return naryOperator(skipKind, e, start)
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
	return naryOperator(mapKind, e, funcWrapper(operand, 1))
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
//  getNames := func(row r.Exp) interface{} {
//      return r.List{row.Attr("name"), row.Attr("real_name")}
//  }
//  err := r.Table("heroes").ConcatMap(getNames).Run(session).All(&names)
//
// Example response:
//
//  ["Captain Britain", "Brian Braddock", "Iceman", "Robert \"Bobby\" Louis Drake", ...]
func (e Exp) ConcatMap(operand interface{}) Exp {
	return naryOperator(concatMapKind, e, funcWrapper(operand, 1))
}

// Filter removes all objects from a sequence that do not match the given
// condition.  The condition can be an RQL expression, an r.Map, or a function
// that returns true or false.
//
// Example with an RQL expression:
//
//   var response []interface{}
//   // Get all heroes with durability 6
//   err := r.Table("heroes").Filter(r.Row.Attr("durability").Eq(6)).Run(session).All(&response)
//
// Example with r.Map:
//
//   err := r.Table("heroes").Filter(r.Map{"durability": 6}).Run(session).All(&response)
//
// Example with function:
//
//   filterFunc := func (row r.Exp) r.Exp { return row.Attr("durability").Eq(6) }
//   err := r.Table("heroes").Filter(filterFunc).Run(session).All(&response)
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
	return naryOperator(filterKind, e, funcWrapper(operand, 1))
}

// HasFields returns true if an object has all the given attributes.
//
// Example usage:
//
//  hero := r.Map{"name": "Iron Man", "energy": 6, "speed": 5}
//  r.Expr(hero).HasFields("energy", "speed") => true
//  r.Expr(hero).HasFields("energy", "guns") => false
func (e Exp) HasFields(keys ...string) Exp {
	return naryOperator(hasFieldsKind, e, stringsToInterfaces(keys)...)
}

// Between gets all rows where the key attribute's value falls between the
// lowerbound and upperbound (inclusive).  Use nil to represent no upper or
// lower bound.  Requires an index on the key (primary keys already have an
// index with the name of the primary key).
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve all heroes with names between "E" and "F"
//   err := r.Table("heroes").Between("name", "E", "F").Run(session).All(&response)
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
func (e Exp) Between(index string, lowerKey, upperKey interface{}) Exp {
	return naryOperator(betweenKind, e, lowerKey, upperKey, index)
}

// LeftBound tells the server when performing a between to include the left endpoint
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve all heroes with names between "E" and "F"
//   err := r.Table("heroes").Between("name", "E", "F").RightBound.Run(session).All(&response)
//
func (e Exp) LeftBound(opt string) Exp {
	return naryOperator(leftboundKind, e, opt)
}

// RightBound tells the server when performing a between to include the right endpoint
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve all heroes with names between "E" and "F"
//   err := r.Table("heroes").Between("name", "E", "F").RightBound.Run(session).All(&response)
//
func (e Exp) RightBound(opt string) Exp {
	return naryOperator(rightboundKind, e, opt)
}

// OrderBy sort the sequence by the values of the given key(s) in each row. The
// default sort is increasing.
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve villains in order of increasing strength
//   err := r.Table("villains").OrderBy("strength").Run(session).All(&response)
//
//   // Retrieve villains in order of decreasing strength, then increasing intelligence
//   query := r.Table("villains").OrderBy(r.Desc("strength"), "intelligence")
//   err := query.Run(session).All(&response)
func (e Exp) OrderBy(orderings ...interface{}) Exp {
	// These are not required to be strings because they could also be
	// orderByAttr structs which specify the direction of sorting
	return naryOperator(orderByKind, e, orderings...)
}

// Asc tells OrderBy to sort a particular attribute in ascending order.  This is
// the default sort.
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve villains in order of increasing fighting ability (worst fighters first)
//   err := r.Table("villains").OrderBy(r.Asc("fighting")).Run(session).All(&response)
func Asc(attr string) Exp {
	return naryOperator(ascendingKind, attr)
}

// Desc tells OrderBy to sort a particular attribute in descending order.
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve villains in order of decreasing speed (fastest villains first)
//   err := r.Table("villains").OrderBy(r.Desc("speed")).Run(session).All(&response)
func Desc(attr string) Exp {
	return naryOperator(descendingKind, attr)
}

// Reduce iterates over a sequence, starting with a base value and applying a
// reduction function to the value so far and the next row of the sequence.
//
// Example usage:
//
//  var sum int
//  // Add the numbers 1-4 together
//  reduction := func(acc, val r.Exp) r.Exp { return acc.Add(val) }
//  err := r.Expr(1,2,3,4).Reduce(reduction, 0).Run(session).One(&sum)
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
//  err := r.Table("heroes").Map(mapping).Reduce(reduction, 0).Run(session).One(&totalSpeed)
//
// Example response:
//
//  232
func (e Exp) Reduce(reduction, base interface{}) Exp {
	return naryOperator(reduceKind, e, funcWrapper(reduction, 2), base)
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
//  query := r.Expr(1,2,3,4,5).GroupedMapReduce(grouping, mapping, reduction, base)
//  err := query.Run(session).One(&response)
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
//  // Group all heroes by intelligence, then find the fastest one in each group
//  grouping := func(row r.Exp) r.Exp { return row.Attr("intelligence") }
//  mapping := func(row r.Exp) r.Exp { return row.Pluck("name", "speed") }
//  base := r.Map{"name": nil, "speed": 0}
//  reduction := func(acc, row r.Exp) r.Exp {
//  	return r.Branch(acc.Attr("speed").Lt(row.Attr("speed")), row, acc)
//  }
//
//  var response []interface{}
//  query := r.Table("heroes").GroupedMapReduce(grouping, mapping, reduction, base)
//  err := query.Run(session).One(&response)
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
func (e Exp) GroupedMapReduce(grouping, mapping, reduction, base interface{}) Exp {
	return naryOperator(groupedMapReduceKind, e, funcWrapper(grouping, 1), funcWrapper(mapping, 1), funcWrapper(reduction, 2), base)
}

/////////////////////
// Derived Methods //
/////////////////////

// Pluck takes only the given attributes from an object, discarding all others.
// See also .Without().
//
// Example usage:
//
//  var heroes []interface{}
//  err := r.Table("heroes").Pluck("real_name", "id").Run(session).All(&heroes)
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
func (e Exp) Pluck(attributes ...interface{}) Exp {
	return naryOperator(pluckKind, e, attributes...)
}

// Without removes the given attributes from an object.  See also .Pluck().
//
// Example usage:
//
//  var heroes []interface{}
//  err := r.Table("heroes").Without("real_name", "id").Run(session).All(&heroes)
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
	return naryOperator(withoutKind, e, stringsToInterfaces(attributes)...)
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
//  compareRows := func (left, right r.Exp) r.Exp {
//      return left.Attr("id").Eq(right.Attr("villain_id"))
//  }
//  err := r.Table("villains").InnerJoin(r.Table("lairs"), compareRows).Run(session).All(&response)
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
func (leftExpr Exp) InnerJoin(rightExpr Exp, predicate interface{}) Exp {
	return naryOperator(innerJoinKind, leftExpr, rightExpr, funcWrapper(predicate, 2))
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
//  compareRows := func (left, right r.Exp) r.Exp {
//      return left.Attr("id").Eq(right.Attr("villain_id"))
//  }
//  err := r.Table("villains").OuterJoin(r.Table("lairs"), compareRows).Run(session).All(&response)
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
func (leftExpr Exp) OuterJoin(rightExpr Exp, predicate interface{}) Exp {
	return naryOperator(outerJoinKind, leftExpr, rightExpr, funcWrapper(predicate, 2))
}

// EqJoin performs a join on two expressions, it is more efficient than
// .InnerJoin() and .OuterJoin() because it looks up elements in the right table
// by primary key. See also .InnerJoin() and .OuterJoin().
//
// Example usage:
//
//  var response []interface{}
//  // Get each hero and their associated lair
//  query := r.Table("villains").EqJoin("id", r.Table("lairs"), "villain_id")
//  err := query.Run(session).All(&response)
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
func (leftExpr Exp) EqJoin(leftAttribute string, rightExpr Exp, index string) Exp {
	return naryOperator(eqJoinKind, leftExpr, leftAttribute, rightExpr, index)
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
//  query := r.Table("heroes").InnerJoin(r.Table("villains"), equalStrength)
//  err := query.Run(session).All(&response)
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
//  query := r.Table("heroes").InnerJoin(r.Table("villains"), equalStrength).Zip()
//  err := query.Run(session).All(&response)
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
	return naryOperator(zipKind, e)
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
func Count() Exp {
	return Expr(Map{"COUNT": true})
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
func Sum(attribute string) Exp {
	return Expr(Map{"SUM": attribute})
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
//      // this is the strength attribute for every member of this group
//      "group": 1,
//      // this is the average value of the intelligence attribute of all members of the group
//      "reduction": 1
//    },
//    {
//      "group": 2,
//      "reduction": 3
//    },
//    ...
//  ]
func Avg(attribute string) Exp {
	return Expr(Map{"AVG": attribute})
}

// DbCreate creates a database with the supplied name.
//
// Example usage:
//
//  err := r.DbCreate("marvel").Run(session).Exec()
func DbCreate(name string) Exp {
	return naryOperator(databaseCreateKind, name)
}

// DbDrop deletes the specified database
//
// Example usage:
//
//  err := r.DbDrop("marvel").Run(session).Exec()
func DbDrop(name string) Exp {
	return naryOperator(databaseDropKind, name)
}

// DbList lists all databases on the server
//
// Example usage:
//
//  var databases []string
//  err := r.DbList().Run(session).All(&databases)
//
// Example response:
//
//  ["test", "marvel"]
func DbList() Exp {
	return Exp{kind: databaseListKind}
}

// Db lets you perform operations within a specific database (this will override
// the database specified on the session).  This can be used to access or
// create/list/delete tables within any database available on the server.
//
// Example usage:
//
//  var response []interface{}
//  // this query will use the default database of the last created session
//  r.Table("test").Run(session).All(&response)
//  // this query will use database "marvel" regardless of what database the session has set
//  r.Db("marvel").Table("heroes").Run(session).All(&response)
func Db(name string) Exp {
	return naryOperator(databaseKind, name)
}

// TableSpec lets you specify the various parameters for a table, then create it
// with TableCreateWithSpec().  See that function for documentation.
type TableSpec struct {
	Name       string
	PrimaryKey string
	Datacenter string
	CacheSize  int64
	Durability string // either "soft" or "hard"
}

// TableCreate creates a table with the specified name.
//
// Example usage:
//
//  err := r.TableCreate("heroes").Run(session).Exec()
func TableCreate(name string) Exp {
	spec := TableSpec{Name: name}
	return naryOperator(tableCreateKind, spec)
}

func (e Exp) TableCreate(name string) Exp {
	spec := TableSpec{Name: name}
	return naryOperator(tableCreateKind, e, spec)
}

// TableCreateWithSpec creates a table with the specified attributes.
//
// Example usage:
//
//  spec := TableSpec{Name: "heroes", PrimaryKey: "name"}
//  err := r.TableCreateWithSpec(spec).Run(session).Exec()
func TableCreateWithSpec(spec TableSpec) Exp {
	return naryOperator(tableCreateKind, spec)
}

func (e Exp) TableCreateWithSpec(spec TableSpec) Exp {
	return naryOperator(tableCreateKind, e, spec)
}

// TableList lists all tables in the database.
//
// Example usage:
//
//  var tables []string
//  err := r.TableList().Run(session).All(&tables)
//
// Example response:
//
//  ["heroes", "villains"]
func TableList() Exp {
	return nullaryOperator(tableListKind)
}

func (e Exp) TableList() Exp {
	return naryOperator(tableListKind, e)
}

// TableDrop removes a table from the database.
//
// Example usage:
//
//  err := r.Db("marvel").TableDrop("heroes").Run(session).Exec()
func TableDrop(name string) Exp {
	return naryOperator(tableDropKind, name)
}

func (e Exp) TableDrop(name string) Exp {
	return naryOperator(tableDropKind, e, name)
}

// Table references all rows in a specific table, using the database that this
// method was called on.
//
// Example usage:
//
//  var response []map[string]interface{}
//  err := r.Db("marvel").Table("heroes").Run(session).All(&response)
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
	return naryOperator(tableKind, name)
}

func (e Exp) Table(name string) Exp {
	return naryOperator(tableKind, e, name)
}

// IndexCreate creates a secondary index on the specified table with the given
// name.  If the function for the index is nil, the index is created for an attribute
// with the same name as the index.
//
// Example usage:
//
//  var response map[string]int
//  err := r.Table("heroes").IndexCreate("name", nil).Run(session).All(&response)
//
// Example response:
//
//  {
//    "created": 1,
//  }
//
// Example usage with function:
//
//  var response map[string]int
//  awesomeness_f := func(hero r.Exp) r.Exp {
//    return hero.Attr("speed").Mul(hero.Attr("strength"))
//  }
//  err := r.Table("heroes").IndexCreate("name", awesomeness_f).Run(session).All(&response)
//
// Example response:
//
//  {
//    "created": 1,
//  }
func (e Exp) IndexCreate(name string, function interface{}) Exp {
	if function == nil {
		return naryOperator(indexCreateKind, e, name)
	}
	return naryOperator(indexCreateKind, e, name, funcWrapper(function, 1))
}

// IndexList lists all secondary indexes on a specified table.
//
// Example usage:
//
//  var response []string
//  err := r.Table("heroes").IndexList().Run(session).One(&response)
//
// Example response:
//
//  ["name", "speed"]
func (e Exp) IndexList() Exp {
	return naryOperator(indexListKind, e)
}

// IndexDrop deletes a secondary index from a table.
//
// Example usage:
//
//  var response map[string]int
//  err := r.Table("heroes").IndexDrop("name").Run(session).One(&response)
//
// Example response:
//
//  {
//    "dropped": 1,
//  }
func (e Exp) IndexDrop(name string) Exp {
	return naryOperator(indexDropKind, e, name)
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
func (e Exp) Insert(rows ...interface{}) Exp {
	return naryOperator(insertKind, e, rows...)
}

// Overwrite tells an Insert query to overwrite existing rows instead of
// returning an error.
//
// Example usage:
//
//  var response r.WriteResponse
//  row := r.Map{"name": "Thing"}
//  err := r.Table("heroes").Insert(row).Overwrite(true).Run(session).One(&response)
func (e Exp) Overwrite(overwrite bool) Exp {
	return naryOperator(upsertKind, e, overwrite)
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
func (e Exp) Atomic(atomic bool) Exp {
	return naryOperator(atomicKind, e, atomic)
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
func (e Exp) Update(mapping interface{}) Exp {
	return naryOperator(updateKind, e, funcWrapper(mapping, 1))
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
func (e Exp) Replace(mapping interface{}) Exp {
	return naryOperator(replaceKind, e, funcWrapper(mapping, 1))
}

// Delete removes one or more rows from the database.
//
// Example usage:
//
//  var response r.WriteResponse
//
//  // Delete a single row by id
//  id := "5d93edbb-2882-4594-8163-f64d8695e575"
//  err := r.Table("heroes").GetById(id).Delete().Run(session).One(&response)
//
//  // Delete all rows in a table
//  err := r.Table("heroes").Delete().Run(session).One(&response)
//
//  // Find a row, then delete it
//  row := r.Map{"real_name": "Peter Benjamin Parker"}
//  err := r.Table("heroes").Filter(row).Delete().Run(session).One(&response)
func (e Exp) Delete() Exp {
	return naryOperator(deleteKind, e)
}

// ForEach runs a given write query for each row of a sequence.
//
// Example usage:
//
//  // Delete all rows with the given ids
//  var response r.WriteResponse
//  // Delete multiple rows by primary key
//  heroNames := []string{"Iron Man", "Colossus"}
//  deleteHero := func (name r.Exp) r.Query {
//      return r.Table("heroes").Get(name, "name").Delete()
//  }
//  err := r.Expr(heroNames).ForEach(deleteHero).Run(session).One(&response)
//
// Example response:
//
//  {
//    "deleted": 2
//  }
func (e Exp) ForEach(queryFunc interface{}) Exp {
	return naryOperator(forEachKind, e, funcWrapper(queryFunc, 1))
}

// Do evalutes the last argument (a function) using all previous arguments as the arguments to the function.
//
// For instance, Do(a, b, c, f) will be run as f(a, b, c).
//
// Example usage:
//
//  var response interface{}
//  err := r.Do(1, 2, 3, func(a, b, c r.Exp) interface{} {
// 	    return r.List{a, b, c}
//  }).Run(session).One(&response)
//
// Example response:
//
// [1,2,3]
func Do(operands ...interface{}) Exp {
	// last argument is a function
	f := operands[len(operands)-1]
	operands = operands[:len(operands)-1]
	return naryOperator(funcallKind, funcWrapper(f, -1), operands...)
}

// TypeOf returns the type of the expression.
//
// Example usage:
//
//  var response string
//  err := r.Expr(1).TypeOf().Run(session).One(&response)
//
// Example response:
//
//  "NUMBER"
func (e Exp) TypeOf() Exp {
	return naryOperator(typeOfKind, e)
}

// Info returns information about the expression.  Often used on tables.
//
// Example usage:
//
//  var response string
//  err := r.Table("heroes").Info().Run(session).One(&response)
//
// Example response:
//
//  "NUMBER"
func (e Exp) Info() Exp {
	return naryOperator(infoKind, e)
}

// CoerceTo converts a value of one type to another type.
//
// You can convert: a selection, sequence, or object into an ARRAY, an array of
// pairs into an OBJECT, and any DATUM into a STRING.
//
// Example usage:
//
//  var response string
//  err := r.Expr(1).CoerceTo("string").Run(session).One(&response)
//
// Example response:
//
//  "1"
func (e Exp) CoerceTo(typename string) Exp {
	return naryOperator(coerceToKind, e, typename)
}

// WithFields filters an array to only include objects with all specified
// fields, then removes all extra fields from each object.
//
// Example usage:
//
//  objects := r.List{
//  	r.Map{"name": "Mono", "sexiness": "maximum"},
//  	r.Map{"name": "Agro", "horseyness": "maximum"},
//  }
//  var response []interface{}
//  r.Expr(objects).WithFields("name", "horseyness").Run(session).One(&response)
//
// Example response:
//
//  {"name": "Agro", "horseyness": "maximum"}
func (e Exp) WithFields(fields ...string) Exp {
	return naryOperator(withFieldsKind, e, stringsToInterfaces(fields)...)
}

// Prepend inserts a value at the beginning of an array.
//
// Example usage:
//
//  var response []string
//  r.Expr(r.List{"a", "b"}).Prepend("z").Run(session).One(&response)
//
// Example response:
//
//  ["z", "a", "b"]
func (e Exp) Prepend(value interface{}) Exp {
	return naryOperator(prependKind, e, value)
}

// InsertAt inserts a single value into an array at the given index.
//
// Example usage:
//
//  var response []string
//  r.Expr(r.List{"a", "b"}).InsertAt(1, "c").Run(session).One(&response)
//
// Example response:
//
//  ["a", "c", "b"]
func (e Exp) InsertAt(index, value interface{}) Exp {
	return naryOperator(insertAtKind, e, index, value)
}

// SpliceAt inserts multiple values into an array at the given index
//
// Example usage:
//
//  var response []string
//  r.Expr(r.List{"a", "b"}).SpliceAt(1, r.List{"c", "a", "t"}).Run(session).One(&response)
//
// Example response:
//
//  ["a", "c", "a", "t", "b"]
func (e Exp) SpliceAt(index, value interface{}) Exp {
	return naryOperator(spliceAtKind, e, index, value)
}

// DeleteAt removes an element from an array from the given start index to the
// end index. If end index is set to nil DeleteAt will only delete
// the element at start index.
//
// Example usage:
//
//  var response []string
//  r.Expr(r.List{"a", "b", "c"}).DeleteAt(1, 2).Run(session).One(&response)
//
// Example response:
//
//  ["a"]
func (e Exp) DeleteAt(startIndex, endIndex interface{}) Exp {
	if endIndex == nil {
		return naryOperator(deleteAtKind, e, startIndex)
	}
	return naryOperator(deleteAtKind, e, startIndex, endIndex)
}

// ChangeAt replaces an element of an array at a given index.
//
// Example usage:
//
//  var response []string
//  r.Expr(r.List{"a", "b", "c"}).ChangeAt(1, "x").Run(session).One(&response)
//
// Example response:
//
//  ["a", "x", "c"]
func (e Exp) ChangeAt(index, value interface{}) Exp {
	return naryOperator(changeAtKind, e, index, value)
}

// Difference removes values from an array.
//
// Example usage:
//
//  var response []string
//  r.Expr(r.List{"a", "b", "b", "c"}).Difference(r.List{"a", "b", "d"}).Run(session).One(&response)
//
// Example response:
//
//  ["c"]
func (e Exp) Difference(value interface{}) Exp {
	return naryOperator(differenceKind, e, value)
}

// IndexesOf gets the indexes where either a specific value appears, or else all
// indexes where the given function returns true.
//
// Example usage:
//
//  var response []int
//  r.Expr(r.List{"a", "b", "b", "a"}).IndexesOf("b").Run(session).One(&response)
//
// Example response:
//
//  [1, 2]
//
// Example usage with function:
//  var response []int
//  r.Expr(r.List{"a", "b", "b", "a"}).IndexesOf(func(row r.Exp) r.Exp {
//  	return r.Expr(row.Eq("b"))
//  }).Run(session).One(&response)
func (e Exp) IndexesOf(operand interface{}) Exp {
	return naryOperator(indexesOfKind, e, funcWrapper(operand, 1))
}

// Keys returns an array of all the keys on an object.
//
// Example usage:
//
//  var response []string
//  expr := r.Expr(r.Map{"name": "rethinkdb", "type": "database"})
//  expr.Keys().Run(session).One(&response)
//
// Example response:
//
//  ["name", "type"]
func (e Exp) Keys() Exp {
	return naryOperator(keysKind, e)
}

// IsEmpty returns true if the sequence is empty.
//
// Example usage:
//
//  var response bool
//  r.Expr(r.List{}).IsEmpty().Run(session).One(&response)
//
// Example response:
//
//  true
func (e Exp) IsEmpty() Exp {
	return naryOperator(isEmptyKind, e)
}

// SetInsert adds a value to an array and returns the unique values of the resulting array.
//
// Example usage:
//
//  var response []string
//  err = r.Expr(r.List{"a", "b", "b"}).SetInsert("b").SetInsert("c").Run(session).One(&response)
//
// Example response:
//
//  ["a", "b", "c"]
func (e Exp) SetInsert(value interface{}) Exp {
	return naryOperator(setInsertKind, e, value)
}

// SetUnion adds multiple values to an array and returns the unique values of the resulting array.
//
// Example usage:
//
//  var response []string
//  err = r.Expr(r.List{"a", "b", "b"}).SetUnion(r.List{"b", "c"}).Run(session).One(&response)
//
// Example response:
//
//  ["a", "b", "c"]
func (e Exp) SetUnion(values interface{}) Exp {
	return naryOperator(setUnionKind, e, values)
}

// SetDifference removes the given values from an array and returns the unique values of the resulting array.
//
// Example usage:
//
//  var response []string
//  err = r.Expr(r.List{"a", "b", "b"}).SetDifference(r.List{"b", "c"}).Run(session).One(&response)
//
// Example response:
//
//  ["a"]
func (e Exp) SetDifference(values interface{}) Exp {
	return naryOperator(setDifferenceKind, e, values)
}

// SetIntersection returns all the unique values that appear in both arrays.
//
// Example usage:
//
//  var response []string
//  err = r.Expr(r.List{"a", "b", "b"}).SetIntersection(r.List{"b", "c"}).Run(session).One(&response)
//
// Example response:
//
//  ["b"]
func (e Exp) SetIntersection(values interface{}) Exp {
	return naryOperator(setIntersectionKind, e, values)
}

// Contains returns true if all the specified values appear in the array, false
// otherwise.
//
// Example usage:
//
//  var response bool
//  err = r.Expr(r.List{"a", "b", "c"}).Contains("a", "b").Run(session).One(&response)
//
// Example response:
//
//  true
func (e Exp) Contains(values ...interface{}) Exp {
	return naryOperator(containsKind, e, values...)
}

// Sample selects a given number of elements from an array randomly with a
// uniform distribution.
//
// Example usage:
//
//  var response []string
//  err = r.Expr(r.List{"a", "b", "c"}).Sample(1).Run(session).One(&response)
//
// Example response:
//
//  ["a"] (maybe)
func (e Exp) Sample(count interface{}) Exp {
	return naryOperator(sampleKind, e, count)
}

// Match matches a regular expression against a string.  The regular expression
// syntax is RE2, which is the same used by the "regexp" package.
//
// Example usage:
//
//  var response interface{}
//  err = r.Expr("3.14159").Match("[0-9]+").Run(session).One(&response)
//
// Example response:
//
//  {"str": "3", "start": 0, "end": 1, "groups": []}
func (e Exp) Match(regularExpression string) Exp {
	return naryOperator(matchKind, e, regularExpression)
}

// Default specifies the default value for an expression if the expression
// evaluates to null or if it raises an error (for instance, a non-existent
// property is accessed).
//
// Example usage:
//
//  var response interface{}
//  r.Expr(r.Map{"a": "b"}).Attr("c").Default("oops").Run(session).One(&response)
//
// Example response:
//
//  "oops"
func (e Exp) Default(value interface{}) Exp {
	return naryOperator(defaultKind, e, value)
}

// ReturnValues tells the server, when performing a single row insert/update/delete/upsert, to return the new and old values on single row
//
// Example usage:
//
//  var response interface{}
//
// Example response:
//
func (e Exp) ReturnValues() Exp {
	return naryOperator(returnValuesKind, e)
}
