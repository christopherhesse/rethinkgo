package rethinkgo

// Convert Expression trees and queries into protocol buffer form
// Functions in this file will panic on failure, the caller is
// expected to recover().

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/json"
	"fmt"
	p "github.com/christopherhesse/rethinkgo/query_language"
	"reflect"
)

// Expressions contain some state that is required when converting them to
// protocol buffers, and has to be passed through.
type context struct {
	databaseName string
	useOutdated  bool
}

func (ctx context) toTerm(o interface{}) *p.Term {
	e := Expr(o)
	value := e.value

	switch e.kind {
	case literalKind:
		return ctx.literalToTerm(value)
	case variableKind:
		return &p.Term{
			Type: p.Term_VAR.Enum(),
			Var:  proto.String(value.(string)),
		}
	case useOutdatedKind:
		useOutdatedArgs := value.(useOutdatedArgs)
		ctx.useOutdated = useOutdatedArgs.useOutdated
		return ctx.toTerm(useOutdatedArgs.expr)
	case implicitVariableKind:
		return &p.Term{
			Type: p.Term_IMPLICIT_VAR.Enum(),
		}
	case letKind:
		letArgs := value.(letArgs)

		return &p.Term{
			Type: p.Term_LET.Enum(),
			Let: &p.Term_Let{
				Binds: ctx.mapToVarTermTuples(letArgs.binds),
				Expr:  ctx.toTerm(letArgs.expr),
			},
		}
	case ifKind:
		ifArgs := value.(ifArgs)

		return &p.Term{
			Type: p.Term_IF.Enum(),
			If_: &p.Term_If{
				Test:        ctx.toTerm(ifArgs.test),
				TrueBranch:  ctx.toTerm(ifArgs.trueBranch),
				FalseBranch: ctx.toTerm(ifArgs.falseBranch),
			},
		}
	case errorKind:
		return &p.Term{
			Type:  p.Term_ERROR.Enum(),
			Error: proto.String(value.(string)),
		}
	case getByKeyKind:
		getArgs := value.(getArgs)
		table, ok := getArgs.table.(tableInfo)
		if !ok {
			panic(".Get() used on something that's not a table")
		}

		return &p.Term{
			Type: p.Term_GETBYKEY.Enum(),
			GetByKey: &p.Term_GetByKey{
				TableRef: ctx.toTableRef(table),
				Attrname: proto.String(getArgs.attribute),
				Key:      ctx.toTerm(getArgs.key),
			},
		}
	case tableKind:
		table := value.(tableInfo)
		return &p.Term{
			Type: p.Term_TABLE.Enum(),
			Table: &p.Term_Table{
				TableRef: ctx.toTableRef(table),
			},
		}
	case javascriptKind:
		return &p.Term{
			Type:       p.Term_JAVASCRIPT.Enum(),
			Javascript: proto.String(value.(string)),
		}
	case groupByKind:
		groupByArgs := value.(groupByArgs)

		grouping := func(row Expression) interface{} {
			return row.Attr(groupByArgs.attribute)
		}
		gmr := groupByArgs.groupedMapReduce

		result := groupByArgs.expression.GroupedMapReduce(
			grouping,
			gmr.Mapping,
			gmr.Base,
			gmr.Reduction,
		)

		finalizer := gmr.Finalizer
		if finalizer != nil {
			finalizerFunc := finalizer.(func(Expression) interface{})
			result = result.Map(func(row Expression) interface{} {
				result := map[string]interface{}{
					"reduction": finalizerFunc(row.Attr("reduction")),
				}
				return row.Merge(result)
			})
		}
		return ctx.toTerm(result)
	}

	// If we're here, the term must be a kind of builtin
	builtinArgs := value.(builtinArgs)

	return &p.Term{
		Type: p.Term_CALL.Enum(),
		Call: &p.Term_Call{
			Builtin: ctx.toBuiltin(e.kind, builtinArgs.operand),
			Args:    ctx.sliceToTerms(builtinArgs.args),
		},
	}
}

func (ctx context) toBuiltin(kind expressionKind, operand interface{}) *p.Builtin {
	var t p.Builtin_BuiltinType

	switch kind {
	case addKind:
		t = p.Builtin_ADD
	case subtractKind:
		t = p.Builtin_SUBTRACT
	case multiplyKind:
		t = p.Builtin_MULTIPLY
	case divideKind:
		t = p.Builtin_DIVIDE
	case moduloKind:
		t = p.Builtin_MODULO
	case logicalAndKind:
		t = p.Builtin_ALL
	case logicalOrKind:
		t = p.Builtin_ANY
	case logicalNotKind:
		t = p.Builtin_NOT
	case arrayToStreamKind:
		t = p.Builtin_ARRAYTOSTREAM
	case streamToArrayKind:
		t = p.Builtin_STREAMTOARRAY
	case mapMergeKind:
		t = p.Builtin_MAPMERGE
	case arrayAppendKind:
		t = p.Builtin_ARRAYAPPEND
	case distinctKind:
		t = p.Builtin_DISTINCT
	case lengthKind:
		t = p.Builtin_LENGTH
	case unionKind:
		t = p.Builtin_UNION
	case nthKind:
		t = p.Builtin_NTH
	case sliceKind:
		t = p.Builtin_SLICE

	case getAttributeKind, implicitGetAttributeKind, hasAttributeKind:
		switch kind {
		case getAttributeKind:
			t = p.Builtin_GETATTR
		case implicitGetAttributeKind:
			t = p.Builtin_IMPLICIT_GETATTR
		case hasAttributeKind:
			t = p.Builtin_HASATTR
		}

		return &p.Builtin{
			Type: t.Enum(),
			Attr: proto.String(operand.(string)),
		}

	case pickAttributesKind, withoutKind:
		switch kind {
		case pickAttributesKind:
			t = p.Builtin_PICKATTRS
		case withoutKind:
			t = p.Builtin_WITHOUT

		}

		return &p.Builtin{
			Type:  t.Enum(),
			Attrs: operand.([]string),
		}

	case filterKind:
		var predicate *p.Predicate
		if reflect.ValueOf(operand).Kind() == reflect.Map {
			// if we get a map like this, the user actually wants to compare
			// individual keys in the document to see if it matches the provided
			// map, build an expression to do that
			predicate = ctx.mapToPredicate(operand)
		} else {
			predicate = ctx.toPredicate(operand)
		}

		return &p.Builtin{
			Type: p.Builtin_FILTER.Enum(),
			Filter: &p.Builtin_Filter{
				Predicate: predicate,
			},
		}

	case orderByKind:
		orderByArgs := operand.(orderByArgs)

		var orderBys []*p.Builtin_OrderBy
		for _, ordering := range orderByArgs.orderings {
			// ascending sort by default
			ascending := true
			attr, ok := ordering.(string)
			if !ok {
				// check if it's the special value returned by asc or dec
				d, ok := ordering.(orderByAttr)
				if !ok {
					panic("Invalid attribute type for OrderBy")
				}
				attr = d.attr
				ascending = d.ascending
			}

			orderBy := &p.Builtin_OrderBy{
				Attr:      proto.String(attr),
				Ascending: proto.Bool(ascending),
			}
			orderBys = append(orderBys, orderBy)
		}

		return &p.Builtin{
			Type:    p.Builtin_ORDERBY.Enum(),
			OrderBy: orderBys,
		}

	case mapKind, concatMapKind:
		mapping := ctx.toMapping(operand)

		if kind == mapKind {
			return &p.Builtin{
				Type: p.Builtin_MAP.Enum(),
				Map: &p.Builtin_Map{
					Mapping: mapping,
				},
			}
		} else { // ConcatMap
			return &p.Builtin{
				Type: p.Builtin_CONCATMAP.Enum(),
				ConcatMap: &p.Builtin_ConcatMap{
					Mapping: mapping,
				},
			}
		}

	case reduceKind:
		reduceArgs := operand.(reduceArgs)
		base := ctx.toTerm(reduceArgs.base)

		return &p.Builtin{
			Type:   p.Builtin_REDUCE.Enum(),
			Reduce: ctx.toReduction(reduceArgs.reduction, base),
		}

	case groupedMapReduceKind:
		groupedMapreduceArgs := operand.(groupedMapReduceArgs)
		base := ctx.toTerm(groupedMapreduceArgs.base)

		return &p.Builtin{
			Type: p.Builtin_GROUPEDMAPREDUCE.Enum(),
			GroupedMapReduce: &p.Builtin_GroupedMapReduce{
				GroupMapping: ctx.toMapping(groupedMapreduceArgs.grouping),
				ValueMapping: ctx.toMapping(groupedMapreduceArgs.mapping),
				Reduction:    ctx.toReduction(groupedMapreduceArgs.reduction, base),
			},
		}

	case rangeKind:
		rangeArgs := operand.(rangeArgs)

		return &p.Builtin{
			Type: p.Builtin_RANGE.Enum(),
			Range: &p.Builtin_Range{
				Attrname:   proto.String(rangeArgs.attrname),
				Lowerbound: ctx.toTerm(rangeArgs.lowerbound),
				Upperbound: ctx.toTerm(rangeArgs.upperbound),
			},
		}

	default:
		return ctx.toComparisonBuiltin(kind)
	}

	return &p.Builtin{
		Type: t.Enum(),
	}
}

func (ctx context) toComparisonBuiltin(kind expressionKind) *p.Builtin {
	var c p.Builtin_Comparison

	switch kind {
	case equalityKind:
		c = p.Builtin_EQ
	case inequalityKind:
		c = p.Builtin_NE
	case greaterThanKind:
		c = p.Builtin_GT
	case greaterThanOrEqualKind:
		c = p.Builtin_GE
	case lessThanKind:
		c = p.Builtin_LT
	case lessThanOrEqualKind:
		c = p.Builtin_LE
	default:
		panic("Unknown expression kind")
	}

	return &p.Builtin{
		Type:       p.Builtin_COMPARE.Enum(),
		Comparison: c.Enum(),
	}
}

var variableNameCounter = 0

func nextVariableName() string {
	variableNameCounter++
	return fmt.Sprintf("arg_%v", variableNameCounter)
}

func (ctx context) compileGoFunc(f interface{}, requiredArgs int) (params []string, body *p.Term) {
	// presumably if we're here, the user has supplied a go func to be
	// converted to an expression
	value := reflect.ValueOf(f)
	type_ := value.Type()

	if type_.NumIn() != requiredArgs {
		panic("Function expression has incorrect number of arguments")
	}

	// check input types and generate the variables to pass to the function
	// the args have generated names because when the function is serialized,
	// the server can't figure out which variable is which in a closure
	var args []reflect.Value
	for i := 0; i < type_.NumIn(); i++ {
		name := nextVariableName()
		args = append(args, reflect.ValueOf(LetVar(name)))
		params = append(params, name)

		// make sure all input arguments are of type Expression
		if !type_.In(i).AssignableTo(reflect.TypeOf(Expression{})) {
			panic("Function argument is not of type Expression")
		}
	}

	// check output types
	if type_.NumOut() != 1 {
		panic("Function does not have a single return value")
	}

	outValue := value.Call(args)[0]
	body = ctx.toTerm(outValue.Interface())
	return
}

func (ctx context) compileExpressionFunc(e Expression, requiredArgs int) (params []string, body *p.Term) {
	// an expression that takes no args, e.g. Row.Attr("name") or
	// possibly a Javascript function JS(`row.key`) which does take args
	body = ctx.toTerm(e)
	switch requiredArgs {
	case 0:
		// do nothing
	case 1:
		params = []string{"row"}
	case 2:
		params = []string{"acc", "row"}
	default:
		panic("This should never happen")
	}
	return
}

func (ctx context) compileFunction(o interface{}, requiredArgs int) (params []string, body *p.Term) {
	e := Expr(o)

	if e.kind == literalKind && reflect.ValueOf(e.value).Kind() == reflect.Func {
		return ctx.compileGoFunc(e.value, requiredArgs)
	}

	return ctx.compileExpressionFunc(e, requiredArgs)
}

func (ctx context) toMapping(o interface{}) *p.Mapping {
	args, body := ctx.compileFunction(o, 1)

	return &p.Mapping{
		Arg:  proto.String(args[0]),
		Body: body,
	}
}

func (ctx context) toPredicate(o interface{}) *p.Predicate {
	args, body := ctx.compileFunction(o, 1)

	return &p.Predicate{
		Arg:  proto.String(args[0]),
		Body: body,
	}
}

func (ctx context) toReduction(o interface{}, base *p.Term) *p.Reduction {
	args, body := ctx.compileFunction(o, 2)

	return &p.Reduction{
		Base: base,
		Var1: proto.String(args[0]),
		Var2: proto.String(args[1]),
		Body: body,
	}
}

func (ctx context) literalToTerm(literal interface{}) *p.Term {
	value := reflect.ValueOf(literal)

	switch value.Kind() {
	case reflect.Array, reflect.Slice:
		return &p.Term{
			Type:  p.Term_ARRAY.Enum(),
			Array: ctx.sliceToTerms(literal),
		}

	case reflect.Map:
		return &p.Term{
			Type:   p.Term_OBJECT.Enum(),
			Object: ctx.mapToVarTermTuples(literal),
		}
	}

	// hopefully it's JSONable
	buf, err := json.Marshal(literal)
	if err != nil {
		panic(err.Error())
	}

	return &p.Term{
		Type:       p.Term_JSON.Enum(),
		Jsonstring: proto.String(string(buf)),
	}
}

func (ctx context) sliceToTerms(a interface{}) (terms []*p.Term) {
	for _, arg := range toArray(a) {
		terms = append(terms, ctx.toTerm(arg))
	}
	return
}

// toArray and toObject seem overly complicated, like maybe some sort
// of assignment assertion would be enough
func toArray(a interface{}) []interface{} {
	result := []interface{}{}

	arrayValue := reflect.ValueOf(a)
	for i := 0; i < arrayValue.Len(); i++ {
		value := arrayValue.Index(i).Interface()
		result = append(result, value)
	}
	return result
}

func toObject(m interface{}) map[string]interface{} {
	result := map[string]interface{}{}

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
		result[key] = value
	}
	return result
}

func (ctx context) mapToPredicate(m interface{}) *p.Predicate {
	var args []interface{}
	for key, value := range toObject(m) {
		args = append(args, Row.Attr(key).Eq(value))
	}

	// And all these terms together
	expr := naryBuiltin(logicalAndKind, nil, args...)
	return ctx.toPredicate(expr)
}

func (ctx context) mapToVarTermTuples(m interface{}) []*p.VarTermTuple {
	var tuples []*p.VarTermTuple
	for key, value := range toObject(m) {
		tuple := &p.VarTermTuple{
			Var:  proto.String(key),
			Term: ctx.toTerm(value),
		}
		tuples = append(tuples, tuple)
	}
	return tuples
}

func (ctx context) toTableRef(table tableInfo) *p.TableRef {
	// Use the context's database name if we didn't specify one
	databaseName := table.database.name
	if databaseName == "" {
		databaseName = ctx.databaseName
	}
	return &p.TableRef{
		TableName:   proto.String(table.name),
		DbName:      proto.String(databaseName),
		UseOutdated: proto.Bool(ctx.useOutdated),
	}
}

func makeMetaQuery(queryType p.MetaQuery_MetaQueryType) *p.Query {
	return &p.Query{
		Type: p.Query_META.Enum(),
		MetaQuery: &p.MetaQuery{
			Type: queryType.Enum(),
		},
	}
}

func makeWriteQuery(queryType p.WriteQuery_WriteQueryType) *p.Query {
	return &p.Query{
		Type: p.Query_WRITE.Enum(),
		WriteQuery: &p.WriteQuery{
			Type: queryType.Enum(),
		},
	}
}

// toProtobuf converts a bare Expression directly to a read query protobuf
func (e Expression) toProtobuf(ctx context) *p.Query {
	return &p.Query{
		Type: p.Query_READ.Enum(),
		ReadQuery: &p.ReadQuery{
			Term: ctx.toTerm(e),
		},
	}
}

// toProtobuf converts a complete query to a protobuf
func (q Query) toProtobuf(ctx context) *p.Query {
	switch v := q.value.(type) {
	case createDatabaseQuery:
		query := makeMetaQuery(p.MetaQuery_CREATE_DB)
		query.MetaQuery.DbName = proto.String(v.name)
		return query

	case dropDatabaseQuery:
		query := makeMetaQuery(p.MetaQuery_DROP_DB)
		query.MetaQuery.DbName = proto.String(v.name)
		return query

	case listDatabasesQuery:
		return makeMetaQuery(p.MetaQuery_LIST_DBS)

	case tableCreateQuery:
		query := makeMetaQuery(p.MetaQuery_CREATE_TABLE)
		query.MetaQuery.CreateTable = &p.MetaQuery_CreateTable{
			PrimaryKey: protoStringOrNil(v.spec.PrimaryKey),
			Datacenter: protoStringOrNil(v.spec.PrimaryDatacenter),
			TableRef: &p.TableRef{
				DbName:    proto.String(v.database.name),
				TableName: proto.String(v.spec.Name),
			},
			CacheSize: protoInt64OrNil(v.spec.CacheSize),
		}
		return query

	case tableListQuery:
		query := makeMetaQuery(p.MetaQuery_LIST_TABLES)
		query.MetaQuery.DbName = proto.String(v.database.name)
		return query

	case tableDropQuery:
		query := makeMetaQuery(p.MetaQuery_DROP_TABLE)
		query.MetaQuery.DropTable = ctx.toTableRef(v.table)
		return query

	case insertQuery:
		var terms []*p.Term
		for _, row := range v.rows {
			terms = append(terms, ctx.toTerm(row))
		}

		table, ok := v.tableExpr.value.(tableInfo)
		if !ok {
			panic("Inserts can only be performed on tables :(")
		}

		query := makeWriteQuery(p.WriteQuery_INSERT)

		query.WriteQuery.Insert = &p.WriteQuery_Insert{
			TableRef:  ctx.toTableRef(table),
			Terms:     terms,
			Overwrite: proto.Bool(v.overwrite),
		}
		return query

	case updateQuery:
		view := ctx.toTerm(v.view)
		mapping := ctx.toMapping(v.mapping)

		if view.GetType() == p.Term_GETBYKEY {
			// this is chained off of a .Get(), do a POINTUPDATE
			query := makeWriteQuery(p.WriteQuery_POINTUPDATE)

			query.WriteQuery.PointUpdate = &p.WriteQuery_PointUpdate{
				TableRef: view.GetByKey.TableRef,
				Attrname: view.GetByKey.Attrname,
				Key:      view.GetByKey.Key,
				Mapping:  mapping,
			}
			return query
		}

		query := makeWriteQuery(p.WriteQuery_UPDATE)

		query.WriteQuery.Update = &p.WriteQuery_Update{
			View:    view,
			Mapping: mapping,
		}
		return query

	case replaceQuery:
		view := ctx.toTerm(v.view)
		mapping := ctx.toMapping(v.mapping)

		if view.GetType() == p.Term_GETBYKEY {
			query := makeWriteQuery(p.WriteQuery_POINTMUTATE)

			query.WriteQuery.PointMutate = &p.WriteQuery_PointMutate{
				TableRef: view.GetByKey.TableRef,
				Attrname: view.GetByKey.Attrname,
				Key:      view.GetByKey.Key,
				Mapping:  mapping,
			}
			return query
		}

		query := makeWriteQuery(p.WriteQuery_MUTATE)

		query.WriteQuery.Mutate = &p.WriteQuery_Mutate{
			View:    view,
			Mapping: mapping,
		}
		return query

	case deleteQuery:
		view := ctx.toTerm(v.view)

		if view.GetType() == p.Term_GETBYKEY {
			query := makeWriteQuery(p.WriteQuery_POINTDELETE)

			query.WriteQuery.PointDelete = &p.WriteQuery_PointDelete{
				TableRef: view.GetByKey.TableRef,
				Attrname: view.GetByKey.Attrname,
				Key:      view.GetByKey.Key,
			}
			return query
		}

		query := makeWriteQuery(p.WriteQuery_DELETE)

		query.WriteQuery.Delete = &p.WriteQuery_Delete{
			View: view,
		}
		return query

	case forEachQuery:
		stream := ctx.toTerm(v.stream)
		name := nextVariableName()
		generatedQuery := v.queryFunc(LetVar(name))
		innerQuery := generatedQuery.toProtobuf(ctx)

		if innerQuery.WriteQuery == nil {
			panic("ForEach query function must generate a write query")
		}

		query := makeWriteQuery(p.WriteQuery_FOREACH)

		query.WriteQuery.ForEach = &p.WriteQuery_ForEach{
			Stream:  stream,
			Var:     proto.String(name),
			Queries: []*p.WriteQuery{innerQuery.WriteQuery},
		}
		return query
	}
	panic("Unknown query type")
}
