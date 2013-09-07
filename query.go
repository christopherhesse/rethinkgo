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
	dateKind
	dayKind
	dayOfWeekKind
	dayOfYearKind
	defaultKind
	deleteAtKind
	deleteKind
	descendingKind
	differenceKind
	distinctKind
	divideKind
	duringKind
	epochTimeKind
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
	hoursKind
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
	inTimezoneKind
	isEmptyKind
	iso8601Kind
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
	mergeLiteralKind
	minutesKind
	moduloKind
	monthKind
	multiplyKind
	nowKind
	nthKind
	orderByKind
	outerJoinKind
	pluckKind
	prependKind
	reduceKind
	replaceKind
	returnValuesKind
	sampleKind
	secondsKind
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
	timeKind
	timeOfDayKind
	timeZoneKind
	toEpochTimeKind
	toIso8601Kind
	typeOfKind
	unionKind
	updateKind
	variableKind
	withFieldsKind
	withoutKind
	yearKind
	zipKind

	mondayKind
	tuesdayKind
	wednesdayKind
	thursdayKind
	fridayKind
	saturdayKind
	sundayKind

	januaryKind
	februaryKind
	marchKind
	aprilKind
	mayKind
	juneKind
	julyKind
	augustKind
	septemberKind
	octoberKind
	novemberKind
	decemberKind

	// custom rethinkgo ones
	upsertKind
	atomicKind
	useOutdatedKind
	durabilityKind
	literalKind
	leftboundKind
	rightboundKind
)

func nullaryOperator(kind expressionKind, args ...interface{}) Exp {
	return Exp{kind: kind, args: args}
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
