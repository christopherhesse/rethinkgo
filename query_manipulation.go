package rethinkgo

// Attr gets an attribute's value from the row.
//
// Example usage:
//
//  r.Expr(r.Map{"key": "value"}).Attr("key") => "value"
func (e Exp) Attr(name string) Exp {
	return naryOperator(getFieldKind, e, name)
}

// Pluck takes only the given attributes from an object, discarding all others.
// See also .Without().
//
// Example usage:
//
//  var heroes []interface{}
//  err := r.Table("heroes").Pluck("real_name", "id").Run(session).All(&heroes)
//
// Example response:
//
//  [
//    {
//      "real_name": "Peter Benjamin Parker",
//      "id": "1227f639-38f0-4cbb-a7b1-9c49f13fe89d",
//    },
//    ...
//  ]
func (e Exp) Pluck(attributes ...interface{}) Exp {
	return naryOperator(pluckKind, e, attributes...)
}

// Without removes the given attributes from an object.  See also .Pluck().
//
// Example usage:
//
//  var heroes []interface{}
//  err := r.Table("heroes").Without("real_name", "id").Run(session).All(&heroes)
//
// Example response:
//
//  [
//    {
//      "durability": 4,
//      "energy": 7,
//      "fighting": 4,
//      "intelligence": 7,
//      "name": "Professor X",
//      "speed": 2,
//      "strength": 4
//    },
//    ...
//  ]
func (e Exp) Without(attributes ...string) Exp {
	return naryOperator(withoutKind, e, stringsToInterfaces(attributes)...)
}

// Merge combines an object with another object, overwriting properties from
// the first with properties from the second.
//
// Example usage:
//
//  var response interface{}
//  firstMap := r.Map{"name": "HAL9000", "role": "Support System"}
//  secondMap := r.Map{"color": "Red", "role": "Betrayal System"}
//  err := r.Expr(firstMap).Merge(secondMap).Run(session).One(&response)
//
// Example response:
//
//  {
//    "color": "Red",
//    "name": "HAL9000",
//    "role": "Betrayal System"
//  }
func (e Exp) Merge(operand interface{}) Exp {
	return naryOperator(mergeKind, e, operand)
}

// Append appends a value to an array.
//
// Example usage:
//
//  var response []interface{}
//  err := r.Expr(r.List{1, 2, 3, 4}).Append(5).Run(session).One(&response)
//
// Example response:
//
//  [1, 2, 3, 4, 5]
func (e Exp) Append(value interface{}) Exp {
	return naryOperator(appendKind, e, value)
}

// Prepend inserts a value at the beginning of an array.
//
// Example usage:
//
//  var response []string
//  r.Expr(r.List{"a", "b"}).Prepend("z").Run(session).One(&response)
//
// Example response:
//
//  ["z", "a", "b"]
func (e Exp) Prepend(value interface{}) Exp {
	return naryOperator(prependKind, e, value)
}

// Difference removes values from an array.
//
// Example usage:
//
//  var response []string
//  r.Expr(r.List{"a", "b", "b", "c"}).Difference(r.List{"a", "b", "d"}).Run(session).One(&response)
//
// Example response:
//
//  ["c"]
func (e Exp) Difference(value interface{}) Exp {
	return naryOperator(differenceKind, e, value)
}

// SetInsert adds a value to an array and returns the unique values of the resulting array.
//
// Example usage:
//
//  var response []string
//  err = r.Expr(r.List{"a", "b", "b"}).SetInsert("b").SetInsert("c").Run(session).One(&response)
//
// Example response:
//
//  ["a", "b", "c"]
func (e Exp) SetInsert(value interface{}) Exp {
	return naryOperator(setInsertKind, e, value)
}

// SetUnion adds multiple values to an array and returns the unique values of the resulting array.
//
// Example usage:
//
//  var response []string
//  err = r.Expr(r.List{"a", "b", "b"}).SetUnion(r.List{"b", "c"}).Run(session).One(&response)
//
// Example response:
//
//  ["a", "b", "c"]
func (e Exp) SetUnion(values interface{}) Exp {
	return naryOperator(setUnionKind, e, values)
}

// SetIntersection returns all the unique values that appear in both arrays.
//
// Example usage:
//
//  var response []string
//  err = r.Expr(r.List{"a", "b", "b"}).SetIntersection(r.List{"b", "c"}).Run(session).One(&response)
//
// Example response:
//
//  ["b"]
func (e Exp) SetIntersection(values interface{}) Exp {
	return naryOperator(setIntersectionKind, e, values)
}

// SetDifference removes the given values from an array and returns the unique values of the resulting array.
//
// Example usage:
//
//  var response []string
//  err = r.Expr(r.List{"a", "b", "b"}).SetDifference(r.List{"b", "c"}).Run(session).One(&response)
//
// Example response:
//
//  ["a"]
func (e Exp) SetDifference(values interface{}) Exp {
	return naryOperator(setDifferenceKind, e, values)
}

// HasFields returns true if an object has all the given attributes.
//
// Example usage:
//
//  hero := r.Map{"name": "Iron Man", "energy": 6, "speed": 5}
//  r.Expr(hero).HasFields("energy", "speed") => true
//  r.Expr(hero).HasFields("energy", "guns") => false
func (e Exp) HasFields(keys ...string) Exp {
	return naryOperator(hasFieldsKind, e, stringsToInterfaces(keys)...)
}

// InsertAt inserts a single value into an array at the given index.
//
// Example usage:
//
//  var response []string
//  r.Expr(r.List{"a", "b"}).InsertAt(1, "c").Run(session).One(&response)
//
// Example response:
//
//  ["a", "c", "b"]
func (e Exp) InsertAt(index, value interface{}) Exp {
	return naryOperator(insertAtKind, e, index, value)
}

// SpliceAt inserts multiple values into an array at the given index
//
// Example usage:
//
//  var response []string
//  r.Expr(r.List{"a", "b"}).SpliceAt(1, r.List{"c", "a", "t"}).Run(session).One(&response)
//
// Example response:
//
//  ["a", "c", "a", "t", "b"]
func (e Exp) SpliceAt(index, value interface{}) Exp {
	return naryOperator(spliceAtKind, e, index, value)
}

// DeleteAt removes an element from an array from the given start index to the
// end index. If end index is set to nil DeleteAt will only delete
// the element at start index.
//
// Example usage:
//
//  var response []string
//  r.Expr(r.List{"a", "b", "c"}).DeleteAt(1, 2).Run(session).One(&response)
//
// Example response:
//
//  ["a"]
func (e Exp) DeleteAt(startIndex, endIndex interface{}) Exp {
	if endIndex == nil {
		return naryOperator(deleteAtKind, e, startIndex)
	}
	return naryOperator(deleteAtKind, e, startIndex, endIndex)
}

// ChangeAt replaces an element of an array at a given index.
//
// Example usage:
//
//  var response []string
//  r.Expr(r.List{"a", "b", "c"}).ChangeAt(1, "x").Run(session).One(&response)
//
// Example response:
//
//  ["a", "x", "c"]
func (e Exp) ChangeAt(index, value interface{}) Exp {
	return naryOperator(changeAtKind, e, index, value)
}

// Keys returns an array of all the keys on an object.
//
// Example usage:
//
//  var response []string
//  expr := r.Expr(r.Map{"name": "rethinkdb", "type": "database"})
//  expr.Keys().Run(session).One(&response)
//
// Example response:
//
//  ["name", "type"]
func (e Exp) Keys() Exp {
	return naryOperator(keysKind, e)
}
