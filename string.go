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
	case implicitGetAttributeKind:
	case hasAttributeKind:
	case pickAttributesKind:
	case mapMergeKind:
	case arrayAppendKind:
	case multiplyKind:
	case divideKind:
	case moduloKind:
	case filterKind:
	case mapKind:
	case concatMapKind:
	case orderByKind:
	case distinctKind:
	case lengthKind:
	case unionKind:
	case nthKind:
	case streamToArrayKind:
	case arrayToStreamKind:
	case reduceKind:
	case groupedMapReduceKind:
	case logicalOrKind:
	case logicalAndKind:
	case rangeKind:
	case withoutKind:
	case equalityKind:
	case inequalityKind:
	case greaterThanKind:
	case greaterThanOrEqualKind:
	case lessThanKind:
	case lessThanOrEqualKind:
	}
	if s == "" {
		return "<unknown builtin>"
	}
	return fmt.Sprintf(s, b.args...)
}

func (gmr GroupedMapReduce) String() string {
	// TODO:
	return "gmr"
}
