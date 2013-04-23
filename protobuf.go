package rethinkgo

// // Convert Exp trees and queries into protocol buffer form.
// // Functions in this file will panic on failure, the caller is expected to
// // recover().

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	p "github.com/christopherhesse/rethinkgo/ql2"
	"reflect"
	"runtime"
	"sync/atomic"
)

// context stores some state that is required when converting Expressions to
// protocol buffers, and has to be passed by value throughout.
type context struct {
	databaseName string
	useOutdated  bool
	overwrite    bool
	atomic       bool
}

// toTerm converts an arbitrary object to a Term, within the context that toTerm
// was called on.
func (ctx context) toTerm(o interface{}) *p.Term {
	e := Expr(o)

	var termType p.Term_TermType
	arguments := e.args
	options := map[string]interface{}{}

	switch e.kind {
	case literalKind:
		return ctx.literalToTerm(e.args[0])
	case variableKind:
		termType = p.Term_VAR
	case javascriptKind:
		termType = p.Term_JAVASCRIPT
	case errorKind:
		termType = p.Term_ERROR
	case implicitVariableKind:
		termType = p.Term_IMPLICIT_VAR

	case databaseKind:
		termType = p.Term_DB
	case tableKind:
		termType = p.Term_TABLE
		// first arg to table must be the database
		if len(arguments) == 1 {
			dbExpr := naryOperator(databaseKind, ctx.databaseName)
			arguments = []interface{}{dbExpr, arguments[0]}
		}
		options["use_outdated"] = ctx.useOutdated
	case getKind:
		termType = p.Term_GET

	case equalityKind:
		termType = p.Term_EQ
	case inequalityKind:
		termType = p.Term_NE
	case lessThanKind:
		termType = p.Term_LT
	case lessThanOrEqualKind:
		termType = p.Term_LE
	case greaterThanKind:
		termType = p.Term_GT
	case greaterThanOrEqualKind:
		termType = p.Term_GE

	case logicalNotKind:
		termType = p.Term_NOT
	case addKind:
		termType = p.Term_ADD
	case subtractKind:
		termType = p.Term_SUB
	case multiplyKind:
		termType = p.Term_MUL
	case divideKind:
		termType = p.Term_DIV
	case moduloKind:
		termType = p.Term_MOD

	case appendKind:
		termType = p.Term_APPEND
	case sliceKind:
		termType = p.Term_SLICE
	case skipKind:
		termType = p.Term_SKIP
	case limitKind:
		termType = p.Term_LIMIT

	case getAttributeKind:
		termType = p.Term_GETATTR
	case containsKind:
		termType = p.Term_CONTAINS
	case pluckKind:
		termType = p.Term_PLUCK
	case withoutKind:
		termType = p.Term_WITHOUT
	case mergeKind:
		termType = p.Term_MERGE

	case betweenKind:
		termType = p.Term_BETWEEN
		options["left_bound"] = arguments[1]
		options["right_bound"] = arguments[2]
		arguments = arguments[:1]
	case reduceKind:
		termType = p.Term_REDUCE
		options["base"] = arguments[2]
		arguments = arguments[:2]
	case mapKind:
		termType = p.Term_MAP
	case filterKind:
		termType = p.Term_FILTER
	case concatMapKind:
		termType = p.Term_CONCATMAP
	case orderByKind:
		termType = p.Term_ORDERBY
	case distinctKind:
		termType = p.Term_DISTINCT
	case countKind:
		termType = p.Term_COUNT
	case unionKind:
		termType = p.Term_UNION
	case nthKind:
		termType = p.Term_NTH
	case groupedMapReduceKind:
		termType = p.Term_GROUPED_MAP_REDUCE
		options["base"] = arguments[4]
		arguments = arguments[:4]
	case groupByKind:
		termType = p.Term_GROUPBY
	case innerJoinKind:
		termType = p.Term_INNER_JOIN
	case outerJoinKind:
		termType = p.Term_OUTER_JOIN
	case eqJoinKind:
		termType = p.Term_EQ_JOIN
	case zipKind:
		termType = p.Term_ZIP

	case coerceToKind:
		termType = p.Term_COERCE_TO
	case typeOfKind:
		termType = p.Term_TYPEOF

	case updateKind:
		termType = p.Term_UPDATE
		options["non_atomic"] = !ctx.atomic
	case deleteKind:
		termType = p.Term_DELETE
	case replaceKind:
		termType = p.Term_REPLACE
		options["non_atomic"] = !ctx.atomic
	case insertKind:
		termType = p.Term_INSERT
		options["upsert"] = ctx.overwrite

	case databaseCreateKind:
		termType = p.Term_DB_CREATE
	case databaseDropKind:
		termType = p.Term_DB_DROP
	case databaseListKind:
		termType = p.Term_DB_LIST
	case tableCreateKind:
		termType = p.Term_TABLE_CREATE
		// last argument is the table spec
		spec := arguments[len(arguments)-1].(TableSpec)
		arguments = arguments[:len(arguments)-1]

		if len(arguments) == 0 {
			// just spec, need to add database
			dbExpr := naryOperator(databaseKind, ctx.databaseName)
			arguments = append(arguments, dbExpr)
		}
		arguments = append(arguments, spec.Name)

		if spec.Datacenter != "" {
			options["datacenter"] = spec.Datacenter
		}
		if spec.PrimaryKey != "" {
			options["primary_key"] = spec.PrimaryKey
		}
		if spec.CacheSize != 0 {
			options["cache_size"] = spec.CacheSize
		}
	case tableDropKind:
		termType = p.Term_TABLE_DROP
		if len(arguments) == 1 {
			// no database specified, use the session database
			dbExpr := naryOperator(databaseKind, ctx.databaseName)
			arguments = []interface{}{dbExpr, arguments[0]}
		}
	case tableListKind:
		termType = p.Term_TABLE_LIST
		if len(arguments) == 0 {
			// no database specified, use the session database
			dbExpr := naryOperator(databaseKind, ctx.databaseName)
			arguments = append(arguments, dbExpr)
		}

	case funcallKind:
		termType = p.Term_FUNCALL
	case branchKind:
		termType = p.Term_BRANCH
	case anyKind:
		termType = p.Term_ANY
	case allKind:
		termType = p.Term_ALL
	case forEachKind:
		termType = p.Term_FOREACH

	case funcKind:
		return ctx.toFuncTerm(arguments[0], arguments[1].(int))
	case ascendingKind:
		termType = p.Term_ASC
	case descendingKind:
		termType = p.Term_DESC

	// special made-up kind to set options on the query
	case upsertKind:
		ctx.overwrite = e.args[1].(bool)
		return ctx.toTerm(e.args[0])
	case atomicKind:
		ctx.atomic = e.args[1].(bool)
		return ctx.toTerm(e.args[0])
	case useOutdatedKind:
		ctx.useOutdated = e.args[1].(bool)
		return ctx.toTerm(e.args[0])
	default:
		panic("invalid term kind")
	}

	args := []*p.Term{}
	for _, arg := range arguments {
		args = append(args, ctx.toTerm(arg))
	}

	var optargs []*p.Term_AssocPair
	for key, value := range options {
		optarg := &p.Term_AssocPair{
			Key: proto.String(key),
			Val: ctx.toTerm(value),
		}
		optargs = append(optargs, optarg)
	}

	return &p.Term{
		Type:    termType.Enum(),
		Args:    args,
		Optargs: optargs,
	}
}

var variableCounter int64 = 0

func nextVariableNumber() int64 {
	return atomic.AddInt64(&variableCounter, 1)
}

func (ctx context) toFuncTerm(f interface{}, requiredArgs int) *p.Term {
	if reflect.ValueOf(f).Kind() == reflect.Func {
		return ctx.compileGoFunc(f, requiredArgs)
	}
	e := Expr(f)
	if e.kind == javascriptKind {
		return ctx.toTerm(e)
	}
	return ctx.compileExpressionFunc(e, requiredArgs)
}

func (ctx context) compileExpressionFunc(e Exp, requiredArgs int) *p.Term {
	// an expression that takes no args, e.g. Row.Attr("name")
	params := []int64{}
	for requiredArgs > 0 {
		params = append(params, nextVariableNumber())
		requiredArgs--
	}

	paramsTerm := ctx.toTerm(params)
	funcTerm := ctx.toTerm(e)

	return &p.Term{
		Type: p.Term_FUNC.Enum(),
		Args: []*p.Term{paramsTerm, funcTerm},
	}
}

func (ctx context) compileGoFunc(f interface{}, requiredArgs int) *p.Term {
	// presumably if we're here, the user has supplied a go func to be
	// converted to an expression
	value := reflect.ValueOf(f)
	valueType := value.Type()

	if requiredArgs != -1 && valueType.NumIn() != requiredArgs {
		panic("Function expression has incorrect number of arguments")
	}

	// check input types and generate the variables to pass to the function
	// the args have generated names because when the function is serialized,
	// the server can't figure out which variable is which in a closure
	var params []int64
	var args []reflect.Value
	for i := 0; i < valueType.NumIn(); i++ {
		number := nextVariableNumber()
		e := naryOperator(variableKind, number)
		args = append(args, reflect.ValueOf(e))
		params = append(params, number)

		// make sure all input arguments are of type Exp
		if !valueType.In(i).AssignableTo(reflect.TypeOf(Exp{})) {
			panic("Function argument is not of type Exp")
		}
	}

	if valueType.NumOut() != 1 {
		panic("Function does not have a single return value")
	}

	outValue := value.Call(args)[0]
	paramsTerm := ctx.toTerm(params)
	funcTerm := ctx.toTerm(outValue.Interface())

	return &p.Term{
		Type: p.Term_FUNC.Enum(),
		Args: []*p.Term{paramsTerm, funcTerm},
	}
}

func (ctx context) literalToTerm(literal interface{}) *p.Term {
	value := reflect.ValueOf(literal)
	var datum *p.Datum

	switch value.Kind() {
	case reflect.Array, reflect.Slice:
		terms := []*p.Term{}
		for _, arg := range toArray(literal) {
			terms = append(terms, ctx.toTerm(arg))
		}

		return &p.Term{
			Type: p.Term_MAKE_ARRAY.Enum(),
			Args: terms,
		}
	case reflect.Map:
		return &p.Term{
			Type:    p.Term_MAKE_OBJ.Enum(),
			Optargs: ctx.mapToAssocPairs(literal),
		}
	}

	datum, err := datumMarshal(literal)
	if err != nil {
		panic(err)
	}

	return &p.Term{
		Type:  p.Term_DATUM.Enum(),
		Datum: datum,
	}
}

// toArray and toObject seem overly complicated, like maybe some sort
// of assignment assertion would be enough
func toArray(a interface{}) []interface{} {
	array := []interface{}{}

	arrayValue := reflect.ValueOf(a)
	for i := 0; i < arrayValue.Len(); i++ {
		value := arrayValue.Index(i).Interface()
		array = append(array, value)
	}
	return array
}

func toObject(m interface{}) map[string]interface{} {
	object := map[string]interface{}{}

	mapValue := reflect.ValueOf(m)
	mapType := mapValue.Type()
	keyType := mapType.Key()

	if keyType.Kind() != reflect.String {
		panic("string keys only in maps")
	}

	for _, keyValue := range mapValue.MapKeys() {
		key := keyValue.String()
		valueValue := mapValue.MapIndex(keyValue)
		value := valueValue.Interface()
		object[key] = value
	}
	return object
}

func (ctx context) mapToAssocPairs(m interface{}) (pairs []*p.Term_AssocPair) {
	for key, value := range toObject(m) {
		pair := &p.Term_AssocPair{
			Key: proto.String(key),
			Val: ctx.toTerm(value),
		}
		pairs = append(pairs, pair)
	}
	return pairs
}

func (e Exp) toProtobuf(ctx context) *p.Query {
	return &p.Query{
		Type:  p.Query_START.Enum(),
		Query: ctx.toTerm(e),
	}
}

// buildProtobuf converts a query to a protobuf and catches any panics raised
// by the toProtobuf() functions.
func (ctx context) buildProtobuf(query Exp) (queryProto *p.Query, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = fmt.Errorf("rethinkdb: %v", r)
		}
	}()

	queryProto = query.toProtobuf(ctx)
	return
}

// Check compiles a query for sending to the server, but does not send it.
// There is one .Check() method for each query type.
func (e Exp) Check(s *Session) error {
	_, err := s.getContext().buildProtobuf(e)
	return err
}
