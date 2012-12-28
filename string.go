package rethinkgo

import (
	"fmt"
)

func (e Expression) String() string {
	switch e.kind {
	case literalKind:
		return fmt.Sprintf(`Expr(%v)`, e.value)
	case groupByKind:
		groupByArgs := e.value.(groupByArgs)
		return fmt.Sprintf(`%v.GroupBy(%v, %v)`, groupByArgs.expr, groupByArgs.attribute, groupByArgs.groupedMapReduce)
	case useOutdatedKind:
		useOutdatedArgs := e.value.(useOutdatedArgs)
		return fmt.Sprintf(`%v.UseOutdated("%v")`, useOutdatedArgs.expr, useOutdatedArgs.useOutdated)
	case variableKind:
		// this needs to be just the variable name so that users can create
		// javascript expressions within functions.
		return e.value.(string)
	case letKind:
		letArgs := e.value.(letArgs)
		return fmt.Sprintf(`Let(%v, %v)`, letArgs.binds, letArgs.expr)
	case ifKind:
		ifArgs := e.value.(ifArgs)
		return fmt.Sprintf(`Branch(%v, %v, %v)`, ifArgs.test, ifArgs.trueBranch, ifArgs.falseBranch)
	case errorKind:
		return fmt.Sprintf(`RuntimeError("%v")`, e.value.(string))
	case getByKeyKind:
		getArgs := e.value.(getArgs)
		return fmt.Sprintf(`%v.Get(%v, "%v")`, getArgs.table, getArgs.key, getArgs.attribute)
	case tableKind:
		tableInfo := e.value.(tableInfo)
		if tableInfo.database.name != "" {
			return fmt.Sprintf(`Db("%v").Table("%v")`, tableInfo.database.name, tableInfo.name)
		} else {
			return fmt.Sprintf(`Table("%v")`, tableInfo.name)
		}
	case javascriptKind:
		return fmt.Sprintf(`Js("%v")`, e.value.(string))
	case implicitVariableKind:
		return "Row"
	default:
		return builtinArgsToString(e)
	}
	return "<unrecognized expression>"
}

func builtinArgsToString(e Expression) string {
	b := e.value.(builtinArgs)
	var s string
	switch e.kind {
	case sliceKind:
		s = `%v.Slice(%v, %v)`
	case addKind:
		s = `%v.Add(%v)`
	case subtractKind:
		s = `%v.Sub(%v)`
	case logicalNotKind:
		s = `%v.Not()`
	case getAttributeKind:
		s = `%v.Attr(%v)`
	case hasAttributeKind:
		s = `%v.Contains(%v)`
	case pickAttributesKind:
		s = `%v.Pick(%v)`
	case mapMergeKind:
		s = `%v.Merge(%v)`
	case arrayAppendKind:
		s = `%v.Append(%v)`
	case multiplyKind:
		s = `%v.Mul(%v)`
	case divideKind:
		s = `%v.Div(%v)`
	case moduloKind:
		s = `%v.Mod(%v)`
	case filterKind:
		s = `%v.Filter(%v)`
	case mapKind:
		s = `%v.Map(%v)`
	case concatMapKind:
		s = `%v.ConcatMap(%v)`
	case orderByKind:
		a := b.operand.(orderByArgs)
		return fmt.Sprintf(`%v.OrderBy(%v)`, b.args[0], a.orderings)
	case distinctKind:
		s = `%v.Distinct(%v)`
	case lengthKind:
		s = `%v.Count()`
	case unionKind:
		s = `%v.Union(%v)`
	case nthKind:
		s = `%v.Nth(%v)`
	case streamToArrayKind:
		s = `%v.StreamToArray()`
	case arrayToStreamKind:
		s = `%v.ArrayToStream()`
	case reduceKind:
		a := b.operand.(reduceArgs)
		return fmt.Sprintf(`%v.Reduce(%v, %v)`, b.args[0], a.base, a.reduction)
	case groupedMapReduceKind:
		a := b.operand.(groupedMapReduceArgs)
		return fmt.Sprintf(`%v.GroupedMapReduce(%v, %v, %v, %v)`, b.args[0], a.grouping, a.mapping, a.base, a.reduction)
	case logicalOrKind:
		s = `%v.Or(%v)`
	case logicalAndKind:
		s = `%v.And(%v)`
	case rangeKind:
		a := b.operand.(rangeArgs)
		return fmt.Sprintf(`%v.Between(%v, %v, %v)`, b.args[0], a.attrname, a.lowerbound, a.upperbound)
	case withoutKind:
		s = `%v.Unpick(%v)`
	case equalityKind:
		s = `%v.Eq(%v)`
	case inequalityKind:
		s = `%v.Ne(%v)`
	case greaterThanKind:
		s = `%v.Gt(%v)`
	case greaterThanOrEqualKind:
		s = `%v.Ge(%v)`
	case lessThanKind:
		s = `%v.Lt(%v)`
	case lessThanOrEqualKind:
		s = `%v.Le(%v)`
	}
	if s == "" {
		return "<unknown builtin>"
	}
	return fmt.Sprintf(s, b.args...)
}

func (q WriteQuery) String() string {
	var s string
	switch v := q.query.(type) {
	case replaceQuery:
		s = fmt.Sprintf(`%v.Replace(%v)`, v.view, v.mapping)
	case forEachQuery:
		s = fmt.Sprintf(`%v.ForEach(%v)`, v.stream, v.queryFunc)
	case deleteQuery:
		s = fmt.Sprintf(`%v.Delete()`, v.view)
	case updateQuery:
		s = fmt.Sprintf(`%v.Update(%v)`, v.view, v.mapping)
	case insertQuery:
		s = fmt.Sprintf(`%v.Insert(%v)`, v.tableExpr, v.rows)
	}
	if q.nonatomic {
		s += ".Atomic(false)"
	}
	if q.overwrite {
		s += ".Overwrite(true)"
	}
	return s
}

func (q MetaQuery) String() string {
	switch v := q.query.(type) {
	case createDatabaseQuery:
		return fmt.Sprintf(`DbCreate("%v")`, v.name)
	case dropDatabaseQuery:
		return fmt.Sprintf(`DbDrop("%v")`, v.name)
	case listDatabasesQuery:
		return `DbList()`
	case tableCreateQuery:
		return fmt.Sprintf(`Db("%v").TableCreate("%v")`, v.database.name, v.spec)
	case tableListQuery:
		return fmt.Sprintf(`Db("%v").TableList()`, v.database.name)
	case tableDropQuery:
		return fmt.Sprintf(`Db("%v").TableDrop("%v")`, v.table.database.name, v.table.name)
	}
	return "<unknown meta query>"
}
