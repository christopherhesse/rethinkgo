package rethinkgo

import (
	"time"
)

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

// RuntimeError tells the server to respond with a ErrRuntime, useful for
// testing.
//
// Example usage:
//
//  err := r.RuntimeError("hi there").Run(session).Err()
func RuntimeError(message string) Exp {
	return Exp{kind: errorKind, args: List{message}}
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
func ExprT(value interface{}) Exp {
	return exprT(value, 20)
	// v, ok := value.(Exp) // check if it's already an Exp
	// if ok {
	// 	return v
	// }
	// return naryOperator(literalKind, value)
}

func exprT(value interface{}, depth int) Exp {
	if depth <= 0 {
		panic("nesting depth limit exceeded")
	}

	switch val := value.(type) {
	case time.Time:
		return nullaryOperator(literalKind, val)
	case Exp:
		return val
	case func() Exp, func(Exp) Exp:
		return funcWrapper(val, 1)
	default:
		return nullaryOperator(literalKind, val)
	}
}

func Expr(value interface{}) Exp {
	return expr(value, 20)
	// v, ok := value.(Exp) // check if it's already an Exp
	// if ok {
	// 	return v
	// }
	// return naryOperator(literalKind, value)
}

func expr(value interface{}, depth int) Exp {
	if depth <= 0 {
		panic("nesting depth limit exceeded")
	}

	switch val := value.(type) {
	default:
		return nullaryOperator(literalKind, val)
	case time.Time:
		return nullaryOperator(literalKind, val)
	case Exp:
		return val
	case List:
	case []Map:
		temp := List{}
		for _, v := range val {
			temp = append(temp, expr(v, depth-1))
		}
		return nullaryOperator(literalKind, temp)
	case Map:
		temp := Map{}
		for k, v := range val {
			temp[k] = expr(v, depth-1)
		}
		return nullaryOperator(literalKind, temp)
	case func() Exp:
	case func(Exp) Exp:
		return funcWrapper(val, 1)
	}

	return nullaryOperator(literalKind, value)
}

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
