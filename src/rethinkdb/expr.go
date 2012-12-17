// Let user create queries as Expression trees, uses interface{} for most types
// which is basically a void* with some runtime checking.

package rethinkdb

type ExpressionKind int

const (
	// These I just made up
	LiteralKind ExpressionKind = iota // converted to an Expression
	GroupByKind

	///////////
	// Terms //
	///////////
	// Null
	VariableKind
	LetKind
	// Call - implicit for all Builtin types
	IfKind
	ErrorKind
	// Number - stored as JSON
	// String - stored as JSON
	// JSON - implicitly specified (use anything that is not Array or Object)
	// Bool - stored as JSON
	// Array - implicitly specified as type []interface{}
	// Object - implicitly specified as type map[string]interface{}
	GetByKeyKind
	TableKind
	JavascriptKind
	ImplicitVariableKind

	//////////////
	// Builtins //
	//////////////
	// these are all subtypes of the CALL term type
	LogicalNotKind
	GetAttributeKind
	ImplicitGetAttributeKind
	HasAttributeKind
	// ImplictHasAttribute - appears to be unused
	PickAttributesKind
	// ImplicitPickAttributes - appears to be unused
	MapMergeKind
	ArrayAppendKind
	SliceKind
	AddKind
	SubtractKind
	MultiplyKind
	DivideKind
	ModuloKind
	// Compare - implicit for all compare subtypes below
	FilterKind
	MapKind
	ConcatMapKind
	OrderByKind
	DistinctKind
	LengthKind
	UnionKind
	NthKind
	StreamToArrayKind
	ArrayToStreamKind
	ReduceKind
	GroupedMapReduceKind
	LogicalOrKind
	LogicalAndKind
	RangeKind
	WithoutKind
	// ImplicitWithout - appears to be unused
	// Compare sub-types
	EqualityKind
	InequalityKind
	GreaterThanKind
	GreaterThanOrEqualKind
	LessThanKind
	LessThanOrEqualKind
)

type Expression struct {
	kind  ExpressionKind
	value interface{}
}

var Row = Expression{kind: ImplicitVariableKind}

func Expr(value interface{}) Expression {
	v, ok := value.(Expression)
	if ok {
		return v
	}
	return Expression{kind: LiteralKind, value: value}
}

// Convenience wrapper for making arrays
func Array(values ...interface{}) Expression {
	return Expr(values)
}

///////////
// Terms //
///////////

func JS(expr string) Expression {
	return Expression{kind: JavascriptKind, value: "return " + expr + ";"}
}

func LetVar(name string) Expression {
	return Expression{kind: VariableKind, value: name}
}

func Error(message string) Expression {
	return Expression{kind: ErrorKind, value: message}
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
	return Expression{kind: IfKind, value: value}
}

type GetArgs struct {
	table     interface{}
	key       Expression
	attribute string
}

func (e Expression) Get(key interface{}, attribute string) Expression {
	value := GetArgs{table: e.value, key: Expr(key), attribute: attribute}
	return Expression{kind: GetByKeyKind, value: value}
}

func (e Expression) GetById(key interface{}) Expression {
	return e.Get(key, "id")
}

type LetArgs struct {
	binds map[string]interface{}
	expr  interface{}
}

func Let(binds map[string]interface{}, expr interface{}) Expression {
	value := LetArgs{
		binds: binds,
		expr:  expr,
	}
	return Expression{kind: LetKind, value: value}
}

type GroupByArgs struct {
	attribute        string
	groupedMapReduce map[string]interface{}
	expression       Expression
}

func (e Expression) GroupBy(attribute string, groupedMapReduce map[string]interface{}) Expression {
	return Expression{
		kind: GroupByKind,
		value: GroupByArgs{
			attribute:        attribute,
			groupedMapReduce: groupedMapReduce,
			expression:       e,
		},
	}
}

//////////////
// Builtins //
//////////////

type BuiltinArgs struct {
	operand interface{}
	args    []interface{}
}

func naryBuiltin(kind ExpressionKind, operand interface{}, args ...interface{}) Expression {
	return Expression{
		kind:  kind,
		value: BuiltinArgs{operand: operand, args: args},
	}
}

func Attr(name string) Expression {
	return naryBuiltin(ImplicitGetAttributeKind, name)
}

func (e Expression) Attr(name string) Expression {
	return naryBuiltin(GetAttributeKind, name, e)
}

func (e Expression) Add(operand interface{}) Expression {
	return naryBuiltin(AddKind, nil, e, operand)
}

func (e Expression) Sub(operand interface{}) Expression {
	return naryBuiltin(SubtractKind, nil, e, operand)
}

func (e Expression) Mul(operand interface{}) Expression {
	return naryBuiltin(MultiplyKind, nil, e, operand)
}

func (e Expression) Div(operand interface{}) Expression {
	return naryBuiltin(DivideKind, nil, e, operand)
}

func (e Expression) Mod(operand interface{}) Expression {
	return naryBuiltin(ModuloKind, nil, e, operand)
}

func (e Expression) And(operand interface{}) Expression {
	return naryBuiltin(LogicalAndKind, nil, e, operand)
}

func (e Expression) Or(operand interface{}) Expression {
	return naryBuiltin(LogicalOrKind, nil, e, operand)
}

func (e Expression) Eq(operand interface{}) Expression {
	return naryBuiltin(EqualityKind, nil, e, operand)
}

func (e Expression) Ne(operand interface{}) Expression {
	return naryBuiltin(InequalityKind, nil, e, operand)
}

func (e Expression) Gt(operand interface{}) Expression {
	return naryBuiltin(GreaterThanKind, nil, e, operand)
}

func (e Expression) Ge(operand interface{}) Expression {
	return naryBuiltin(GreaterThanOrEqualKind, nil, e, operand)
}

func (e Expression) Lt(operand interface{}) Expression {
	return naryBuiltin(LessThanKind, nil, e, operand)
}

func (e Expression) Le(operand interface{}) Expression {
	return naryBuiltin(LessThanOrEqualKind, nil, e, operand)
}

func (e Expression) Not() Expression {
	return naryBuiltin(LogicalNotKind, nil, e)
}

func (e Expression) ArrayToStream() Expression {
	return naryBuiltin(ArrayToStreamKind, nil, e)
}

func (e Expression) StreamToArray() Expression {
	return naryBuiltin(StreamToArrayKind, nil, e)
}

func (e Expression) Distinct() Expression {
	return naryBuiltin(DistinctKind, nil, e)
}

func (e Expression) Count() Expression {
	return naryBuiltin(LengthKind, nil, e)
}

func (e Expression) Merge(operand interface{}) Expression {
	return naryBuiltin(MapMergeKind, nil, e, operand)
}

func (e Expression) Append(operand interface{}) Expression {
	return naryBuiltin(ArrayAppendKind, nil, e, operand)
}

func (e Expression) Union(operands ...interface{}) Expression {
	return naryBuiltin(UnionKind, nil, e, operands)
}

func (e Expression) Nth(operand interface{}) Expression {
	return naryBuiltin(NthKind, nil, e, operand)
}

func (e Expression) Slice(lower, upper interface{}) Expression {
	return naryBuiltin(SliceKind, nil, e, lower, upper)
}

func (e Expression) Limit(limit interface{}) Expression {
	return e.Slice(0, limit)
}

func (e Expression) Skip(start interface{}) Expression {
	return e.Slice(start, nil)
}

func (e Expression) Map(operand interface{}) Expression {
	return naryBuiltin(MapKind, operand, e)
}

func (e Expression) ConcatMap(operand interface{}) Expression {
	return naryBuiltin(ConcatMapKind, operand, e)
}

func (e Expression) Filter(operand interface{}) Expression {
	return naryBuiltin(FilterKind, operand, e)
}

func (e Expression) Contains(key string) Expression {
	return naryBuiltin(HasAttributeKind, key, e)
}

func (e Expression) Pick(attributes ...string) Expression {
	return naryBuiltin(PickAttributesKind, attributes, e)
}

func (e Expression) Unpick(attributes ...string) Expression {
	return naryBuiltin(WithoutKind, attributes, e)
}

type RangeArgs struct {
	attrname   string
	lowerbound interface{}
	upperbound interface{}
}

func (e Expression) Between(attrname string, lowerbound, upperbound interface{}) Expression {
	operand := RangeArgs{
		attrname:   attrname,
		lowerbound: lowerbound,
		upperbound: upperbound,
	}

	return naryBuiltin(RangeKind, operand, e)
}

func (e Expression) BetweenIds(lowerbound, upperbound interface{}) Expression {
	return e.Between("id", lowerbound, upperbound)
}

type OrderByArgs struct {
	orderings []interface{}
}

func (e Expression) OrderBy(orderings ...interface{}) Expression {
	// These are not required to be strings because they could also be
	// OrderByAttr structs which specify the direction of sorting
	operand := OrderByArgs{
		orderings: orderings,
	}
	return naryBuiltin(OrderByKind, operand, e)
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
	operand := ReduceArgs{
		base:      base,
		reduction: reduction,
	}
	return naryBuiltin(ReduceKind, operand, e)
}

type GroupedMapReduceArgs struct {
	grouping  interface{}
	mapping   interface{}
	base      interface{}
	reduction interface{}
}

func (e Expression) GroupedMapReduce(grouping, mapping, base, reduction interface{}) Expression {
	operand := GroupedMapReduceArgs{
		grouping:  grouping,
		mapping:   mapping,
		base:      base,
		reduction: reduction,
	}
	return naryBuiltin(GroupedMapReduceKind, operand, e)
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

func Count() map[string]interface{} {
	return map[string]interface{}{
		"mapping":   func(row Expression) interface{} { return 1 },
		"base":      0,
		"reduction": func(acc, val Expression) interface{} { return acc.Add(val) },
	}
}

func Sum(attribute string) map[string]interface{} {
	return map[string]interface{}{
		"mapping":   func(row Expression) interface{} { return row.Attr(attribute) },
		"base":      0,
		"reduction": func(acc, val Expression) interface{} { return acc.Add(val) },
	}
}

func Avg(attribute string) map[string]interface{} {
	return map[string]interface{}{
		"mapping": func(row Expression) interface{} { return []interface{}{row.Attr(attribute), 1} },
		"base":    []int{0, 0},
		"reduction": func(acc, val Expression) interface{} {
			return []interface{}{
				acc.Nth(0).Add(val.Nth(0)),
				acc.Nth(1).Add(val.Nth(1)),
			}
		},
		"finalizer": func(row Expression) interface{} {
			return row.Nth(0).Div(row.Nth(1))
		},
	}
}
