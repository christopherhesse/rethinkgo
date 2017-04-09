package rethinkgo

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
