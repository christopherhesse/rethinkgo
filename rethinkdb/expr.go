package rethinkdb

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/json"
	"errors"
	"reflect"
	p "rethinkdb/query_language"
)

type ExpressionKind int

const (
	// These I just made up
	Constant ExpressionKind = iota // marshals a value to a JSON term
	Function                       // converted to an Expression

	///////////
	// Terms //
	///////////
	// Null - use JSON instead
	Variable
	Let
	// Call - implicit for all Builtin types
	If
	Error
	// Number - use JSON instead
	// String - use JSON instead
	// JSON - use JSON instead
	// Bool - use JSON instead
	// Array - use JSON instead
	// Object - use JSON instead
	GetByKey
	Table
	Javascript
	ImplicitVariable

	//////////////
	// Builtins //
	//////////////
	// these are all subtypes of the CALL term type
	LogicalNot
	GetAttribute
	ImplicitGetAttribute
	HasAttribute
	// ImplictHasAttribute - appears to be unused
	PickAttributes
	// ImplicitPickAttributes - appears to be unused
	MapMerge
	ArrayAppend
	Slice
	Add
	Subtract
	Multiply
	Divide
	Modulo
	// Compare - implicit for all compare subtypes below
	Filter
	Map
	ConcatMap
	OrderBy
	Distinct
	Length
	Union
	Nth
	StreamToArray
	ArrayToStream
	Reduce
	GroupedMapReduce
	LogicalOr
	LogicalAnd
	Range
	Without
	// ImplicitWithout - appears to be unused
	// Compare sub-types
	Equality
	Inequality
	GreaterThan
	GreaterThanOrEqual
	LessThan
	LessThanOrEqual
)

type Expression struct {
	kind  ExpressionKind
	value interface{}
	args  []interface{} // used for builtins
}

// Convert a bare Expression directly to a read query
func (e Expression) buildProtobuf() (*p.Query, error) {
	term, err := e.Term()
	if err != nil {
		return nil, err
	}

	return &p.Query{
		Type: p.Query_READ.Enum(),
		ReadQuery: &p.ReadQuery{
			Term: term,
		},
	}, nil
}

func toTerms(args []interface{}) (terms []*p.Term, err error) {
	var term *p.Term
	for _, arg := range args {
		term, err = Expr(arg).Term()
		if err != nil {
			return
		}
		terms = append(terms, term)
	}
	return
}

func (e Expression) Term() (term *p.Term, err error) {
	switch e.kind {
	case Constant:
		// check if it's a function being converted to an expression
		v := reflect.ValueOf(e.value)
		if v.Kind() == reflect.Func {
			f1, ok := e.value.(func(Expression) Expression)
			if ok {
				return f1(LetVar("row")).Term()
			}

			f2 := e.value.(func(Expression, Expression) Expression)
			return f2(LetVar("acc"), LetVar("row")).Term()
		}

		buf, err := json.Marshal(e.value)
		if err != nil {
			return nil, err
		}

		term = &p.Term{
			Type:       p.Term_JSON.Enum(),
			Jsonstring: proto.String(string(buf)),
		}
	case Variable:
		name := e.value.(string)
		term = &p.Term{Type: p.Term_VAR.Enum(), Var: proto.String(name)}
	case ImplicitVariable:
		term = &p.Term{Type: p.Term_IMPLICIT_VAR.Enum()}
	case Let:
		letArgs := e.value.(LetArgs)

		expr, err := Expr(letArgs.expr).Term()
		if err != nil {
			return nil, err
		}

		var binds []*p.VarTermTuple
		for key, value := range letArgs.binds {
			term, err := Expr(value).Term()
			if err != nil {
				return nil, err
			}

			bind := &p.VarTermTuple{
				Var:  proto.String(key),
				Term: term,
			}
			binds = append(binds, bind)
		}

		term = &p.Term{
			Type: p.Term_LET.Enum(),
			Let: &p.Term_Let{
				Binds: binds,
				Expr:  expr,
			},
		}
	case If:
		ifArgs := e.value.(IfArgs)

		test, err := Expr(ifArgs.test).Term()
		if err != nil {
			return nil, err
		}

		trueBranch, err := Expr(ifArgs.trueBranch).Term()
		if err != nil {
			return nil, err
		}

		falseBranch, err := Expr(ifArgs.falseBranch).Term()
		if err != nil {
			return nil, err
		}

		term = &p.Term{
			Type: p.Term_IF.Enum(),
			If_: &p.Term_If{
				Test:        test,
				TrueBranch:  trueBranch,
				FalseBranch: falseBranch,
			},
		}
	case Error:
		message := e.value.(string)
		term = &p.Term{
			Type:  p.Term_ERROR.Enum(),
			Error: proto.String(message),
		}
	case GetByKey:
		getArgs := e.value.(GetArgs)

		table, ok := getArgs.table.(TableInfo)
		if !ok {
			return nil, errors.New("rethinkdb: .Get() used on something that's not a table")
		}

		key, err := getArgs.key.Term()
		if err != nil {
			return nil, err
		}

		term = &p.Term{
			Type: p.Term_GETBYKEY.Enum(),
			GetByKey: &p.Term_GetByKey{
				TableRef: table.toTableRef(),
				Attrname: proto.String(getArgs.attribute),
				Key:      key,
			},
		}
	case Table:
		t := e.value.(TableInfo)
		term = &p.Term{
			Type: p.Term_TABLE.Enum(),
			Table: &p.Term_Table{
				TableRef: t.toTableRef(),
			},
		}
	case Javascript:
		s := e.value.(string)
		term = &p.Term{
			Type:       p.Term_JAVASCRIPT.Enum(),
			Javascript: proto.String(s),
		}
	default:
		// If we're here, the term is actually a builtin
		args, err := toTerms(e.args)
		if err != nil {
			return nil, err
		}
		builtin, err := convertBuiltin(e)
		if err != nil {
			return nil, err
		}

		term = &p.Term{
			Type: p.Term_CALL.Enum(),
			Call: &p.Term_Call{
				Builtin: builtin,
				Args:    args,
			},
		}
	}
	return
}

func convertBuiltin(e Expression) (builtin *p.Builtin, err error) {
	var t p.Builtin_BuiltinType

	switch e.kind {
	case Add:
		t = p.Builtin_ADD
	case Subtract:
		t = p.Builtin_SUBTRACT
	case Multiply:
		t = p.Builtin_MULTIPLY
	case Divide:
		t = p.Builtin_DIVIDE
	case Modulo:
		t = p.Builtin_MODULO
	case LogicalAnd:
		t = p.Builtin_ALL
	case LogicalOr:
		t = p.Builtin_ANY
	case LogicalNot:
		t = p.Builtin_NOT
	case ArrayToStream:
		t = p.Builtin_ARRAYTOSTREAM
	case StreamToArray:
		t = p.Builtin_STREAMTOARRAY
	case MapMerge:
		t = p.Builtin_MAPMERGE
	case ArrayAppend:
		t = p.Builtin_ARRAYAPPEND
	case Distinct:
		t = p.Builtin_DISTINCT
	case Length:
		t = p.Builtin_LENGTH
	case Union:
		t = p.Builtin_UNION
	case Nth:
		t = p.Builtin_NTH
	case Slice:
		t = p.Builtin_SLICE

	case GetAttribute, ImplicitGetAttribute, HasAttribute:
		switch e.kind {
		case GetAttribute:
			t = p.Builtin_GETATTR
		case ImplicitGetAttribute:
			t = p.Builtin_IMPLICIT_GETATTR
		case HasAttribute:
			t = p.Builtin_HASATTR
		}

		name := e.value.(string)
		builtin = &p.Builtin{
			Type: t.Enum(),
			Attr: proto.String(name),
		}
		return

	case PickAttributes, Without:
		attributes := e.value.([]string)
		switch e.kind {
		case PickAttributes:
			t = p.Builtin_PICKATTRS
		case Without:
			t = p.Builtin_WITHOUT

		}
		builtin = &p.Builtin{
			Type:  t.Enum(),
			Attrs: attributes,
		}
		return

	case Filter:
		var expr Expression
		m, ok := e.value.(map[string]interface{})
		if ok {
			// if we get a map like this, the user actually wants to compare
			// individual keys in the document to see if it matches the provided
			// map
			var args []interface{}
			for key, value := range m {
				args = append(args, Attr(key).Eq(value))
			}
			expr = Expression{kind: LogicalAnd, args: args}

		} else {
			expr = Expr(e.value)
		}

		body, err := expr.Term()
		if err != nil {
			return nil, err
		}

		builtin = &p.Builtin{
			Type: p.Builtin_FILTER.Enum(),
			Filter: &p.Builtin_Filter{
				Predicate: &p.Predicate{
					Arg:  proto.String("row"),
					Body: body,
				},
			},
		}
		return builtin, err

	case OrderBy:
		orderByArgs := e.value.(OrderByArgs)

		var orderBys []*p.Builtin_OrderBy
		for _, ordering := range orderByArgs.orderings {
			var attr string
			attr, ok := ordering.(string)
			// ascending sort by default
			ascending := true

			if !ok {
				// check if it's the special value returned by asc or dec
				d, ok := ordering.(OrderByAttr)
				if !ok {
					return nil, errors.New("rethinkdb: Invalid attribute/sort")
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

		builtin = &p.Builtin{
			Type:    p.Builtin_ORDERBY.Enum(),
			OrderBy: orderBys,
		}
		return

	case Map, ConcatMap:
		body, err := Expr(e.value).Term()
		if err != nil {
			return nil, err
		}

		mapping := &p.Mapping{
			Arg:  proto.String("row"),
			Body: body,
		}

		if e.kind == Map {
			builtin = &p.Builtin{
				Type: p.Builtin_MAP.Enum(),
				Map: &p.Builtin_Map{
					Mapping: mapping,
				},
			}
		} else { // ConcatMap
			builtin = &p.Builtin{
				Type: p.Builtin_CONCATMAP.Enum(),
				ConcatMap: &p.Builtin_ConcatMap{
					Mapping: mapping,
				},
			}
		}
		return builtin, err

	case Reduce:
		reduceArgs := e.value.(ReduceArgs)

		base, err := Expr(reduceArgs.base).Term()
		if err != nil {
			return nil, err
		}

		body, err := Expr(reduceArgs.reduction).Term()
		if err != nil {
			return nil, err
		}

		builtin = &p.Builtin{
			Type: p.Builtin_REDUCE.Enum(),
			Reduce: &p.Reduction{
				Base: base,
				Var1: proto.String("acc"),
				Var2: proto.String("row"),
				Body: body,
			},
		}
		return builtin, err

	case GroupedMapReduce:
		groupedMapReduceArgs := e.value.(GroupedMapReduceArgs)

		groupMappingBody, err := Expr(groupedMapReduceArgs.grouping).Term()
		if err != nil {
			return nil, err
		}

		groupMapping := &p.Mapping{
			Arg:  proto.String("row"),
			Body: groupMappingBody,
		}

		valueMappingBody, err := Expr(groupedMapReduceArgs.mapping).Term()
		if err != nil {
			return nil, err
		}

		valueMapping := &p.Mapping{
			Arg:  proto.String("row"),
			Body: valueMappingBody,
		}

		base, err := Expr(groupedMapReduceArgs.base).Term()
		if err != nil {
			return nil, err
		}

		reductionBody, err := Expr(groupedMapReduceArgs.reduction).Term()
		if err != nil {
			return nil, err
		}

		reduction := &p.Reduction{
			Base: base,
			Var1: proto.String("acc"),
			Var2: proto.String("row"),
			Body: reductionBody,
		}

		builtin = &p.Builtin{
			Type: p.Builtin_GROUPEDMAPREDUCE.Enum(),
			GroupedMapReduce: &p.Builtin_GroupedMapReduce{
				GroupMapping: groupMapping,
				ValueMapping: valueMapping,
				Reduction:    reduction,
			},
		}
		return builtin, err

	case Range:
		rangeArgs := e.value.(RangeArgs)

		lowerbound, err := Expr(rangeArgs.lowerbound).Term()
		if err != nil {
			return nil, err
		}

		upperbound, err := Expr(rangeArgs.upperbound).Term()
		if err != nil {
			return nil, err
		}

		builtin = &p.Builtin{
			Type: p.Builtin_RANGE.Enum(),
			Range: &p.Builtin_Range{
				Attrname:   proto.String(rangeArgs.attrname),
				Lowerbound: lowerbound,
				Upperbound: upperbound,
			},
		}
		return builtin, err

	default:
		var c p.Builtin_Comparison

		switch e.kind {
		case Equality:
			c = p.Builtin_EQ
		case Inequality:
			c = p.Builtin_NE
		case GreaterThan:
			c = p.Builtin_GT
		case GreaterThanOrEqual:
			c = p.Builtin_GE
		case LessThan:
			c = p.Builtin_LT
		case LessThanOrEqual:
			c = p.Builtin_LE
		default:
			panic("rethinkdb: Unknown expression kind")
		}

		builtin = &p.Builtin{
			Type:       p.Builtin_COMPARE.Enum(),
			Comparison: c.Enum(),
		}
		return
	}

	builtin = &p.Builtin{
		Type: t.Enum(),
	}
	return
}

///////////
// Terms //
///////////

func Expr(value interface{}) Expression {
	v, ok := value.(Expression)
	if ok {
		return v
	}
	return Expression{kind: Constant, value: value}
}

func JS(expr string) Expression {
	return Expression{kind: Javascript, value: "return " + expr + ";"}
}

func LetVar(name string) Expression {
	if name == "@" {
		return Expression{kind: ImplicitVariable}
	}
	return Expression{kind: Variable, value: name}
}

func Err(message string) Expression {
	return Expression{kind: Error, value: message}
}

type IfArgs struct {
	test        interface{}
	trueBranch  interface{}
	falseBranch interface{}
}

func Branch(test, trueBranch, falseBranch interface{}) Expression {
	value := IfArgs{
		test:        test,
		trueBranch:  trueBranch,
		falseBranch: falseBranch,
	}
	return Expression{kind: If, value: value}
}

//////////////
// Builtins //
//////////////

func Attr(name string) Expression {
	return Expression{kind: ImplicitGetAttribute, value: name}
}

func (e Expression) Attr(name string) Expression {
	return Expression{
		kind:  GetAttribute,
		value: name,
		args:  []interface{}{e},
	}
}

func naryBuiltin(kind ExpressionKind, args ...interface{}) Expression {
	return Expression{
		kind: kind,
		args: args,
	}
}

func (e Expression) Add(operand interface{}) Expression {
	return naryBuiltin(Add, e, operand)
}

func (e Expression) Sub(operand interface{}) Expression {
	return naryBuiltin(Subtract, e, operand)
}

func (e Expression) Mul(operand interface{}) Expression {
	return naryBuiltin(Multiply, e, operand)
}

func (e Expression) Div(operand interface{}) Expression {
	return naryBuiltin(Divide, e, operand)
}

func (e Expression) Mod(operand interface{}) Expression {
	return naryBuiltin(Modulo, e, operand)
}

func (e Expression) And(operand interface{}) Expression {
	return naryBuiltin(LogicalAnd, e, operand)
}

func (e Expression) Or(operand interface{}) Expression {
	return naryBuiltin(LogicalOr, e, operand)
}

func (e Expression) Eq(operand interface{}) Expression {
	return naryBuiltin(Equality, e, operand)
}

func (e Expression) Ne(operand interface{}) Expression {
	return naryBuiltin(Inequality, e, operand)
}

func (e Expression) Gt(operand interface{}) Expression {
	return naryBuiltin(GreaterThan, e, operand)
}

func (e Expression) Ge(operand interface{}) Expression {
	return naryBuiltin(GreaterThanOrEqual, e, operand)
}

func (e Expression) Lt(operand interface{}) Expression {
	return naryBuiltin(LessThan, e, operand)
}

func (e Expression) Le(operand interface{}) Expression {
	return naryBuiltin(LessThanOrEqual, e, operand)
}

func (e Expression) Not() Expression {
	return naryBuiltin(LogicalNot, e)
}

func (e Expression) ArrayToStream() Expression {
	return naryBuiltin(ArrayToStream, e)
}

func (e Expression) StreamToArray() Expression {
	return naryBuiltin(StreamToArray, e)
}

func (e Expression) Distinct() Expression {
	return naryBuiltin(Distinct, e)
}

func (e Expression) Length() Expression {
	return naryBuiltin(Length, e)
}

func (e Expression) Extend(operand interface{}) Expression {
	return naryBuiltin(MapMerge, e, operand)
}

func (e Expression) Append(operand interface{}) Expression {
	return naryBuiltin(ArrayAppend, e, operand)
}

func (e Expression) Union(operands ...interface{}) Expression {
	return naryBuiltin(Union, e, operands)
}

func (e Expression) Nth(operand interface{}) Expression {
	return naryBuiltin(Nth, e, operand)
}

func (e Expression) Slice(lower, upper interface{}) Expression {
	return naryBuiltin(Slice, e, lower, upper)
}

func (e Expression) Map(operand interface{}) Expression {
	return Expression{kind: Map, value: operand, args: []interface{}{e}}
}

func (e Expression) ConcatMap(operand interface{}) Expression {
	return Expression{kind: ConcatMap, value: operand, args: []interface{}{e}}
}

func (e Expression) Filter(operand interface{}) Expression {
	return Expression{kind: Filter, value: operand, args: []interface{}{e}}
}

func (e Expression) Contains(key string) Expression {
	return Expression{kind: HasAttribute, value: key, args: []interface{}{e}}
}

func (e Expression) Pick(attributes ...string) Expression {
	return Expression{kind: PickAttributes, value: attributes, args: []interface{}{e}}
}

func (e Expression) Unpick(attributes ...string) Expression {
	return Expression{kind: Without, value: attributes, args: []interface{}{e}}
}

type GetArgs struct {
	// This must be a table expression, but since errors are defered until we
	// compile the whole expression, we don't check now
	table     interface{}
	key       Expression
	attribute string
}

func (e Expression) Get(key interface{}, attribute string) Expression {
	value := GetArgs{table: e.value, key: Expr(key), attribute: attribute}
	return Expression{kind: GetByKey, value: value}
}

func (e Expression) GetById(key interface{}) Expression {
	return e.Get(key, "id")
}

type LetArgs struct {
	binds map[string]interface{}
	expr  interface{}
}

func (e Expression) Let(binds map[string]interface{}, expr interface{}) Expression {
	value := LetArgs{
		binds: binds,
		expr:  expr,
	}
	return Expression{kind: Let, value: value, args: []interface{}{e}}
}

type RangeArgs struct {
	attrname   string
	lowerbound interface{}
	upperbound interface{}
}

func (e Expression) Between(attrname string, lowerbound, upperbound interface{}) Expression {
	value := RangeArgs{
		attrname:   attrname,
		lowerbound: lowerbound,
		upperbound: upperbound,
	}
	return Expression{kind: Range, value: value, args: []interface{}{e}}
}

func (e Expression) BetweenIds(lowerbound, upperbound interface{}) Expression {
	return e.Between("id", lowerbound, upperbound)
}

type OrderByArgs struct {
	orderings []interface{}
}

func (e Expression) OrderBy(orderings ...interface{}) Expression {
	value := OrderByArgs{
		orderings: orderings,
	}
	return Expression{kind: OrderBy, value: value, args: []interface{}{e}}
}

type OrderByAttr struct {
	attr      string
	ascending bool
}

func Asc(attr string) OrderByAttr {
	return OrderByAttr{attr, true}
}

func Desc(attr string) OrderByAttr {
	return OrderByAttr{attr, false}
}

type ReduceArgs struct {
	base      interface{}
	reduction interface{}
}

func (e Expression) Reduce(base, reduction interface{}) Expression {
	value := ReduceArgs{
		base:      base,
		reduction: reduction,
	}
	return Expression{kind: Reduce, value: value, args: []interface{}{e}}
}

type GroupedMapReduceArgs struct {
	grouping  interface{}
	mapping   interface{}
	base      interface{}
	reduction interface{}
}

func (e Expression) GroupedMapReduce(grouping, mapping, base, reduction interface{}) Expression {
	value := GroupedMapReduceArgs{
		grouping:  grouping,
		mapping:   mapping,
		base:      base,
		reduction: reduction,
	}
	return Expression{kind: GroupedMapReduce, value: value, args: []interface{}{e}}
}
