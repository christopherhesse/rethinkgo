// Convert Expression trees and queries into protocol buffer form
// Functions in this file will panic on failure, the caller is expected
// to recover().

package rethinkdb

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/json"
	"fmt"
	"reflect"
	p "rethinkdb/query_language"
	"runtime"
)

func toTerm(o interface{}) *p.Term {
	e := Expr(o)
	value := e.value

	switch e.kind {
	case LiteralKind:
		return literalToTerm(value)
	case VariableKind:
		return &p.Term{
			Type: p.Term_VAR.Enum(),
			Var:  proto.String(value.(string)),
		}
	case ImplicitVariableKind:
		return &p.Term{
			Type: p.Term_IMPLICIT_VAR.Enum(),
		}
	case LetKind:
		letArgs := value.(LetArgs)

		return &p.Term{
			Type: p.Term_LET.Enum(),
			Let: &p.Term_Let{
				Binds: mapToVarTermTuples(letArgs.binds),
				Expr:  toTerm(letArgs.expr),
			},
		}
	case IfKind:
		ifArgs := value.(IfArgs)

		return &p.Term{
			Type: p.Term_IF.Enum(),
			If_: &p.Term_If{
				Test:        toTerm(ifArgs.test),
				TrueBranch:  toTerm(ifArgs.trueBranch),
				FalseBranch: toTerm(ifArgs.falseBranch),
			},
		}
	case ErrorKind:
		return &p.Term{
			Type:  p.Term_ERROR.Enum(),
			Error: proto.String(value.(string)),
		}
	case GetByKeyKind:
		getArgs := value.(GetArgs)
		table, ok := getArgs.table.(TableInfo)
		if !ok {
			panic(".Get() used on something that's not a table")
		}

		return &p.Term{
			Type: p.Term_GETBYKEY.Enum(),
			GetByKey: &p.Term_GetByKey{
				TableRef: table.toTableRef(),
				Attrname: proto.String(getArgs.attribute),
				Key:      toTerm(getArgs.key),
			},
		}
	case TableKind:
		table := value.(TableInfo)
		return &p.Term{
			Type: p.Term_TABLE.Enum(),
			Table: &p.Term_Table{
				TableRef: table.toTableRef(),
			},
		}
	case JavascriptKind:
		return &p.Term{
			Type:       p.Term_JAVASCRIPT.Enum(),
			Javascript: proto.String(value.(string)),
		}
	case GroupByKind:
		groupByArgs := value.(GroupByArgs)

		grouping := func(row Expression) interface{} {
			return row.Attr(groupByArgs.attribute)
		}
		gmr := groupByArgs.groupedMapReduce
		mapping := gmr["mapping"]
		base := gmr["base"]
		reduction := gmr["reduction"]

		result := groupByArgs.expression.GroupedMapReduce(
			grouping,
			mapping,
			base,
			reduction,
		)

		finalizer := gmr["finalizer"]
		if finalizer != nil {
			finalizerFunc := finalizer.(func(Expression) interface{})
			result = result.Map(func(row Expression) interface{} {
				result := map[string]interface{}{
					"reduction": finalizerFunc(row.Attr("reduction")),
				}
				return row.Merge(result)
			})
		}
		return toTerm(result)
	}

	// If we're here, the term must be a kind of builtin
	builtinArgs := value.(BuiltinArgs)

	return &p.Term{
		Type: p.Term_CALL.Enum(),
		Call: &p.Term_Call{
			Builtin: toBuiltin(e.kind, builtinArgs.operand),
			Args:    sliceToTerms(builtinArgs.args),
		},
	}
}

func toBuiltin(kind ExpressionKind, operand interface{}) *p.Builtin {
	var t p.Builtin_BuiltinType

	switch kind {
	case AddKind:
		t = p.Builtin_ADD
	case SubtractKind:
		t = p.Builtin_SUBTRACT
	case MultiplyKind:
		t = p.Builtin_MULTIPLY
	case DivideKind:
		t = p.Builtin_DIVIDE
	case ModuloKind:
		t = p.Builtin_MODULO
	case LogicalAndKind:
		t = p.Builtin_ALL
	case LogicalOrKind:
		t = p.Builtin_ANY
	case LogicalNotKind:
		t = p.Builtin_NOT
	case ArrayToStreamKind:
		t = p.Builtin_ARRAYTOSTREAM
	case StreamToArrayKind:
		t = p.Builtin_STREAMTOARRAY
	case MapMergeKind:
		t = p.Builtin_MAPMERGE
	case ArrayAppendKind:
		t = p.Builtin_ARRAYAPPEND
	case DistinctKind:
		t = p.Builtin_DISTINCT
	case LengthKind:
		t = p.Builtin_LENGTH
	case UnionKind:
		t = p.Builtin_UNION
	case NthKind:
		t = p.Builtin_NTH
	case SliceKind:
		t = p.Builtin_SLICE

	case GetAttributeKind, ImplicitGetAttributeKind, HasAttributeKind:
		switch kind {
		case GetAttributeKind:
			t = p.Builtin_GETATTR
		case ImplicitGetAttributeKind:
			t = p.Builtin_IMPLICIT_GETATTR
		case HasAttributeKind:
			t = p.Builtin_HASATTR
		}

		return &p.Builtin{
			Type: t.Enum(),
			Attr: proto.String(operand.(string)),
		}

	case PickAttributesKind, WithoutKind:
		switch kind {
		case PickAttributesKind:
			t = p.Builtin_PICKATTRS
		case WithoutKind:
			t = p.Builtin_WITHOUT

		}

		return &p.Builtin{
			Type:  t.Enum(),
			Attrs: operand.([]string),
		}

	case FilterKind:
		var expr Expression
		var predicate *p.Predicate
		// TODO: should this also work with, for instance map[string]string?
		m, ok := operand.(map[string]interface{})
		if ok {
			// if we get a map like this, the user actually wants to compare
			// individual keys in the document to see if it matches the provided
			// map, build an expression to do that
			var args []interface{}
			for key, value := range m {
				args = append(args, Attr(key).Eq(value))
			}
			expr = naryBuiltin(LogicalAndKind, nil, args...)
			body := toTerm(expr)
			predicate = &p.Predicate{
				Arg:  proto.String("row"),
				Body: body,
			}
		} else {
			expr = Expr(operand)
			predicate = toPredicate(expr)
		}

		return &p.Builtin{
			Type: p.Builtin_FILTER.Enum(),
			Filter: &p.Builtin_Filter{
				Predicate: predicate,
			},
		}

	case OrderByKind:
		orderByArgs := operand.(OrderByArgs)

		var orderBys []*p.Builtin_OrderBy
		for _, ordering := range orderByArgs.orderings {
			// ascending sort by default
			ascending := true
			attr, ok := ordering.(string)
			if !ok {
				// check if it's the special value returned by asc or dec
				d, ok := ordering.(OrderByAttr)
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

	case MapKind, ConcatMapKind:
		mapping := toMapping(operand)

		if kind == MapKind {
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

	case ReduceKind:
		reduceArgs := operand.(ReduceArgs)
		base := toTerm(reduceArgs.base)

		return &p.Builtin{
			Type:   p.Builtin_REDUCE.Enum(),
			Reduce: toReduction(reduceArgs.reduction, base),
		}

	case GroupedMapReduceKind:
		groupedMapReduceArgs := operand.(GroupedMapReduceArgs)
		base := toTerm(groupedMapReduceArgs.base)

		return &p.Builtin{
			Type: p.Builtin_GROUPEDMAPREDUCE.Enum(),
			GroupedMapReduce: &p.Builtin_GroupedMapReduce{
				GroupMapping: toMapping(groupedMapReduceArgs.grouping),
				ValueMapping: toMapping(groupedMapReduceArgs.mapping),
				Reduction:    toReduction(groupedMapReduceArgs.reduction, base),
			},
		}

	case RangeKind:
		rangeArgs := operand.(RangeArgs)

		return &p.Builtin{
			Type: p.Builtin_RANGE.Enum(),
			Range: &p.Builtin_Range{
				Attrname:   proto.String(rangeArgs.attrname),
				Lowerbound: toTerm(rangeArgs.lowerbound),
				Upperbound: toTerm(rangeArgs.upperbound),
			},
		}

	default:
		return toComparisonBuiltin(kind)
	}

	return &p.Builtin{
		Type: t.Enum(),
	}
}

func toComparisonBuiltin(kind ExpressionKind) *p.Builtin {
	var c p.Builtin_Comparison

	switch kind {
	case EqualityKind:
		c = p.Builtin_EQ
	case InequalityKind:
		c = p.Builtin_NE
	case GreaterThanKind:
		c = p.Builtin_GT
	case GreaterThanOrEqualKind:
		c = p.Builtin_GE
	case LessThanKind:
		c = p.Builtin_LT
	case LessThanOrEqualKind:
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

func compileGoFunc(f interface{}, requiredArgs int) (params []string, body *p.Term) {
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
	body = toTerm(outValue.Interface())
	return
}

func compileExpressionFunc(e Expression, requiredArgs int) (params []string, body *p.Term) {
	// an expression that takes no args, e.g. LetVar("@").Attr("name") or
	// possibly a Javascript function JS(`row.key`) which does take args
	body = toTerm(e)
	// TODO: see if this is required (maybe check js library as python seems to have this)
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

func compileFunction(o interface{}, requiredArgs int) (params []string, body *p.Term) {
	e := Expr(o)

	if e.kind == LiteralKind && reflect.ValueOf(e.value).Kind() == reflect.Func {
		return compileGoFunc(e.value, requiredArgs)
	}

	return compileExpressionFunc(e, requiredArgs)
}

func toMapping(o interface{}) *p.Mapping {
	args, body := compileFunction(o, 1)

	return &p.Mapping{
		Arg:  proto.String(args[0]),
		Body: body,
	}
}

func toPredicate(o interface{}) *p.Predicate {
	args, body := compileFunction(o, 1)

	return &p.Predicate{
		Arg:  proto.String(args[0]),
		Body: body,
	}
}

func toReduction(o interface{}, base *p.Term) *p.Reduction {
	args, body := compileFunction(o, 2)

	return &p.Reduction{
		Base: base,
		Var1: proto.String(args[0]),
		Var2: proto.String(args[1]),
		Body: body,
	}
}

func literalToTerm(literal interface{}) *p.Term {
	value := reflect.ValueOf(literal)

	switch value.Kind() {
	case reflect.Array, reflect.Slice:
		values, ok := literal.([]interface{})
		if !ok {
			// Nope, try JSON encoder instead
			break
		}

		return &p.Term{
			Type:  p.Term_ARRAY.Enum(),
			Array: sliceToTerms(values),
		}

	case reflect.Map:
		m, ok := literal.(map[string]interface{})
		if !ok {
			// Nope, try JSON encoder
			break
		}

		return &p.Term{
			Type:   p.Term_OBJECT.Enum(),
			Object: mapToVarTermTuples(m),
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

func sliceToTerms(args []interface{}) (terms []*p.Term) {
	for _, arg := range args {
		terms = append(terms, toTerm(arg))
	}
	return
}

func mapToVarTermTuples(m map[string]interface{}) []*p.VarTermTuple {
	var tuples []*p.VarTermTuple
	for key, value := range m {
		tuple := &p.VarTermTuple{
			Var:  proto.String(key),
			Term: toTerm(value),
		}
		tuples = append(tuples, tuple)
	}
	return tuples
}

// Calls toTerm() on the object, and returns any panics as normal errors
func buildTerm(o interface{}) (term *p.Term, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = fmt.Errorf("rethinkdb: %v", r)
		}
	}()
	return toTerm(o), nil
}

// Calls toMapping() on the object, and returns any panics as normal errors
func buildMapping(o interface{}) (mapping *p.Mapping, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = fmt.Errorf("rethinkdb: %v", r)
		}
	}()
	return toMapping(o), nil
}

// Convert a bare Expression directly to a read query
func (e Expression) buildProtobuf() (query *p.Query, err error) {
	term, err := buildTerm(e)
	if err != nil {
		return
	}

	query = &p.Query{
		Type: p.Query_READ.Enum(),
		ReadQuery: &p.ReadQuery{
			Term: term,
		},
	}
	return
}
