package rethinkgo

// Map transforms a sequence by applying the given function to each row.
//
// Example usage:
//
//  var squares []int
//  // Square a series of numbers
//  square := func(row r.Exp) r.Exp { return row.Mul(row) }
//  err := r.Expr(1,2,3).Map(square).Run(session).One(&squares)
//
// Example response:
//
//  [1, 2, 3]
//
// Example usage:
//
//  var heroes []interface{}
//  // Fetch multiple rows by primary key
//  heroNames := []string{"Iron Man", "Colossus"}
//  getHero := func (name r.Exp) r.Exp { return r.Table("heroes").Get(name, "name") }
//  err := r.Expr(heroNames).Map(getHero).Run(session).One(&heroes)
//
// Example response:
//
//  [
//    {
//      "durability": 6,
//      "energy": 6,
//      "fighting": 3,
//      "intelligence": 6,
//      "name": "Iron Man",
//      "real_name": "Anthony Edward \"Tony\" Stark",
//      "speed": 5,
//      "strength": 6
//    },
//    ...
//  ]
func (e Exp) Map(operand interface{}) Exp {
	return naryOperator(mapKind, e, funcWrapper(operand, 1))
}

// WithFields filters an array to only include objects with all specified
// fields, then removes all extra fields from each object.
//
// Example usage:
//
//  objects := r.List{
//  	r.Map{"name": "Mono", "sexiness": "maximum"},
//  	r.Map{"name": "Agro", "horseyness": "maximum"},
//  }
//  var response []interface{}
//  r.Expr(objects).WithFields("name", "horseyness").Run(session).One(&response)
//
// Example response:
//
//  {"name": "Agro", "horseyness": "maximum"}
func (e Exp) WithFields(fields ...string) Exp {
	return naryOperator(withFieldsKind, e, stringsToInterfaces(fields)...)
}

// ConcatMap constructs a sequence by running the provided function on each row,
// then concatenating all the results.
//
// Example usage:
//
//  var flattened []int
//  // Flatten some nested lists
//  flatten := func(row r.Exp) r.Exp { return row }
//  err := r.Expr(r.List{1,2}, r.List{3,4}).ConcatMap(flatten).Run(session).One(&flattened)
//
// Example response:
//
//  [1, 2, 3, 4]
//
// Example usage:
//
//  var names []string
//  // Get all hero real names and aliases in a list
//  getNames := func(row r.Exp) interface{} {
//      return r.List{row.Attr("name"), row.Attr("real_name")}
//  }
//  err := r.Table("heroes").ConcatMap(getNames).Run(session).All(&names)
//
// Example response:
//
//  ["Captain Britain", "Brian Braddock", "Iceman", "Robert \"Bobby\" Louis Drake", ...]
func (e Exp) ConcatMap(operand interface{}) Exp {
	return naryOperator(concatMapKind, e, funcWrapper(operand, 1))
}

// OrderBy sort the sequence by the values of the given key(s) in each row. The
// default sort is increasing.
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve villains in order of increasing strength
//   err := r.Table("villains").OrderBy("strength").Run(session).All(&response)
//
//   // Retrieve villains in order of decreasing strength, then increasing intelligence
//   query := r.Table("villains").OrderBy(r.Desc("strength"), "intelligence")
//   err := query.Run(session).All(&response)
//
//   query := r.Table("villains").OrderBy(func(row r.Exp) r.Exp {
//       return row.Attr("strength")
//   }, "intelligence")
//   err := query.Run(session).All(&response)
func (e Exp) OrderBy(orderings ...interface{}) Exp {
	for i, ordering := range orderings {
		switch ordering.(type) {
		case Exp:
			if exp := ordering.(Exp); !(exp.kind == descendingKind || exp.kind == ascendingKind) {
				orderings[i] = funcWrapper(ordering, 1)
			}
		default:
			orderings[i] = funcWrapper(ordering, 1)
		}
	}

	// These are not required to be strings because they could also be
	// orderByAttr structs which specify the direction of sorting
	return naryOperator(orderByKind, e, orderings...)
}

// Asc tells OrderBy to sort a particular attribute in ascending order.  This is
// the default sort.
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve villains in order of increasing fighting ability (worst fighters first)
//   err := r.Table("villains").OrderBy(r.Asc("fighting")).Run(session).All(&response)
func Asc(attr interface{}) Exp {
	return naryOperator(ascendingKind, funcWrapper(attr, 1))
}

// Desc tells OrderBy to sort a particular attribute in descending order.
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve villains in order of decreasing speed (fastest villains first)
//   err := r.Table("villains").OrderBy(r.Desc("speed")).Run(session).All(&response)
func Desc(attr interface{}) Exp {
	return naryOperator(descendingKind, funcWrapper(attr, 1))
}

// Skip returns all results after the first `start` results.  Basically it's the
// opposite of .Limit().
//
// Example usage:
//
//  var response []int
//  err := r.Expr(1,2,3,4,5).Skip(3).Run(session).One(&response)
//
// Example response:
//
//  [4, 5]
func (e Exp) Skip(start interface{}) Exp {
	return naryOperator(skipKind, e, start)
}

// Limit returns only the first `limit` results from the query.
//
// Example usage:
//
//  var response []int
//  err := r.Expr(1,2,3,4,5).Limit(3).Run(session).One(&response)
//
// Example response:
//
//  [1, 2, 3]
func (e Exp) Limit(limit interface{}) Exp {
	return naryOperator(limitKind, e, limit)
}

// Slice returns a section of a sequence, with bounds [lower, upper), where
// lower bound is inclusive and upper bound is exclusive.
//
// Example usage:
//
//  var response []int
//  err := r.Expr(1,2,3,4,5).Slice(2,4).Run(session).One(&response)
//
// Example response:
//
//  [3, 4]
func (e Exp) Slice(lower, upper interface{}) Exp {
	return naryOperator(sliceKind, e, lower, upper)
}

// Nth returns the nth element in sequence, zero-indexed.
//
// Example usage:
//
//  var response int
//  // Get the second element of an array
//  err := r.Expr(4,3,2,1).Nth(1).Run(session).One(&response)
//
// Example response:
//
//  3
func (e Exp) Nth(operand interface{}) Exp {
	return naryOperator(nthKind, e, operand)
}

// IndexesOf gets the indexes where either a specific value appears, or else all
// indexes where the given function returns true.
//
// Example usage:
//
//  var response []int
//  r.Expr(r.List{"a", "b", "b", "a"}).IndexesOf("b").Run(session).One(&response)
//
// Example response:
//
//  [1, 2]
//
// Example usage with function:
//  var response []int
//  r.Expr(r.List{"a", "b", "b", "a"}).IndexesOf(func(row r.Exp) r.Exp {
//  	return r.Expr(row.Eq("b"))
//  }).Run(session).One(&response)
func (e Exp) IndexesOf(operand interface{}) Exp {
	return naryOperator(indexesOfKind, e, funcWrapper(operand, 1))
}

// IsEmpty returns true if the sequence is empty.
//
// Example usage:
//
//  var response bool
//  r.Expr(r.List{}).IsEmpty().Run(session).One(&response)
//
// Example response:
//
//  true
func (e Exp) IsEmpty() Exp {
	return naryOperator(isEmptyKind, e)
}

// Union concatenates two sequences.
//
// Example usage:
//
//  var response []interface{}
//  // Retrieve all heroes and villains
//  r.Table("heroes").Union(r.Table("villains")).Run(session).All(&response)
//
// Example response:
//
//  [
//    {
//      "durability": 6,
//      "energy": 6,
//      "fighting": 3,
//      "id": "1a760d0b-57ef-42a8-9fec-c3a1f34930aa",
//      "intelligence": 6,
//      "name": "Iron Man",
//      "real_name": "Anthony Edward \"Tony\" Stark",
//      "speed": 5,
//      "strength": 6
//    },
//    ...
//  ]
func (e Exp) Union(operands ...interface{}) Exp {
	return naryOperator(unionKind, e, operands...)
}

// Sample selects a given number of elements from an array randomly with a
// uniform distribution.
//
// Example usage:
//
//  var response []string
//  err = r.Expr(r.List{"a", "b", "c"}).Sample(1).Run(session).One(&response)
//
// Example response:
//
//  ["a"] (maybe)
func (e Exp) Sample(count interface{}) Exp {
	return naryOperator(sampleKind, e, count)
}
