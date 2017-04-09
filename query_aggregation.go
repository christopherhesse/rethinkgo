package rethinkgo

// Count counts the number of rows in a group, for use with the .GroupBy()
// method.
//
// Example usage:
//
//  var response []interface{}
//  // Count all heroes in each superhero group
//  err := r.Table("heroes").GroupBy("affiliation", r.Count()).Run(session).One(&response)
//
// Example response:
//
//  [
//    {
//      "group": "Avengers", // this is the affiliation attribute for every member of this group
//      "reduction": 9  // this is the number of members in this group
//    },
//    {
//      "group": "X-Men",
//      "reduction": 12
//    },
//    ...
//  ]
func Count() Exp {
	return Expr(Map{"COUNT": true})
}

// Sum computes the sum of an attribute for a group, for use with the .GroupBy()
// method.
//
// Example usage:
//
//  var response []interface{}
//  // Get the total intelligence of all heroes who have the same strength
//  err := r.Table("heroes").GroupBy("strength", r.Sum("intelligence")).Run(session).One(&response)
//
// Example response:
//
//  [
//    {
//      // this is the strength attribute for every member of this group
//      "group": 1,
//      // this is the sum of the intelligence attribute of all members of the group
//      "reduction": 2
//    },
//    {
//      "group": 2,
//      "reduction": 15
//    },
//    ...
//  ]
func Sum(attribute string) Exp {
	return Expr(Map{"SUM": attribute})
}

// Avg computes the average value of an attribute for a group, for use with the
// .GroupBy() method.
//
// Example usage:
//
//  var response []interface{}
//  err := r.Table("heroes").GroupBy("strength", r.Avg("intelligence")).Run(session).One(&response)
//
// Example response:
//
//  [
//    {
//      // this is the strength attribute for every member of this group
//      "group": 1,
//      // this is the average value of the intelligence attribute of all members of the group
//      "reduction": 1
//    },
//    {
//      "group": 2,
//      "reduction": 3
//    },
//    ...
//  ]
func Avg(attribute string) Exp {
	return Expr(Map{"AVG": attribute})
}

// Reduce iterates over a sequence, starting with a base value and applying a
// reduction function to the value so far and the next row of the sequence.
//
// Example usage:
//
//  var sum int
//  // Add the numbers 1-4 together
//  reduction := func(acc, val r.Exp) r.Exp { return acc.Add(val) }
//  err := r.Expr(1,2,3,4).Reduce(reduction, 0).Run(session).One(&sum)
//
// Example response:
//
//  10
//
// Example usage:
//
//  var totalSpeed int
//  // Compute the total speed for all heroes, the types of acc and val should
//  // be the same, so we extract the speed first with a .Map()
//  mapping := func(row r.Exp) r.Exp { return row.Attr("speed") }
//  reduction := func(acc, val r.Exp) r.Exp { return acc.Add(val) }
//  err := r.Table("heroes").Map(mapping).Reduce(reduction, 0).Run(session).One(&totalSpeed)
//
// Example response:
//
//  232
func (e Exp) Reduce(reduction, base interface{}) Exp {
	return naryOperator(reduceKind, e, funcWrapper(reduction, 2), base)
}

// Count returns the number of elements in the response.
//
// Example usage:
//
//  var response int
//  err := r.Table("heroes").Count().Run(session).One(&response)
//
// Example response:
//
//  42
func (e Exp) Count() Exp {
	return naryOperator(countKind, e)
}

// Distinct removes duplicate elements from a sequence.
//
// Example usage:
//
//  var response []interface{}
//  // Get a list of all possible strength values for our heroes
//  err := r.Table("heroes").Map(r.Row.Attr("strength")).Distinct().Run(session).All(&response)
//
// Example response:
//
//  [7, 1, 6, 4, 2, 5, 3]
func (e Exp) Distinct() Exp {
	return naryOperator(distinctKind, e)
}

// GroupedMapReduce partitions a sequence into groups, then performs a map and a
// reduction on each group.  See also .Map() and .GroupBy().
//
// Example usage:
//
//  // Find the sum of the even and odd numbers separately
//  grouping := func(row r.Exp) r.Exp { return r.Branch(row.Mod(2).Eq(0), "even", "odd") }
//  mapping := func(row r.Exp) r.Exp { return row }
//  base := 0
//  reduction := func(acc, row r.Exp) r.Exp {
//  	return acc.Add(row)
//  }
//
//  var response []interface{}
//  query := r.Expr(1,2,3,4,5).GroupedMapReduce(grouping, mapping, reduction, base)
//  err := query.Run(session).One(&response)
//
// Example response:
//
//  [
//    {
//      "group": "even",
//      "reduction": 6
//    },
//    {
//      "group": "odd",
//      "reduction": 9
//    }
//  ]
//
// Example usage:
//
//  // Group all heroes by intelligence, then find the fastest one in each group
//  grouping := func(row r.Exp) r.Exp { return row.Attr("intelligence") }
//  mapping := func(row r.Exp) r.Exp { return row.Pluck("name", "speed") }
//  base := r.Map{"name": nil, "speed": 0}
//  reduction := func(acc, row r.Exp) r.Exp {
//  	return r.Branch(acc.Attr("speed").Lt(row.Attr("speed")), row, acc)
//  }
//
//  var response []interface{}
//  query := r.Table("heroes").GroupedMapReduce(grouping, mapping, reduction, base)
//  err := query.Run(session).One(&response)
//
// Example response:
//
//  [
//    {
//      "group": 1,
//      "reduction": {
//        "name": "Northstar",
//        "speed": 2
//      }
//    },
//    {
//      "group": 2,
//      "reduction": {
//        "name": "Thor",
//        "speed": 6
//      }
//    },
//    ...
//  ]
func (e Exp) GroupedMapReduce(grouping, mapping, reduction, base interface{}) Exp {
	return naryOperator(groupedMapReduceKind, e, funcWrapper(grouping, 1), funcWrapper(mapping, 1), funcWrapper(reduction, 2), base)
}

// GroupBy does a sort of grouped map reduce.  First the server groups all rows
// that have the same value for `attribute`, then it applys the map reduce to
// each group.  It takes one of the following reductions: r.Count(),
// r.Sum(string), r.Avg(string)
//
// `attribute` must be a single attribute (string) or a list of attributes
// ([]string)
//
// Example usage:
//
//  var response []interface{}
//  // Find all heroes with the same durability, calculate their average speed
//  // to see if more durable heroes are slower.
//  err := r.Table("heroes").GroupBy("durability", r.Avg("speed")).Run(session).One(&response)
//
// Example response:
//
//  [
//    {
//      "group": 1,  // this is the strength attribute for every member of this group
//      "reduction": 1.5  // this is the sum of the intelligence attribute of all members of the group
//    },
//    {
//      "group": 2,
//      "reduction": 3.5
//    },
//    ...
//  ]
//
// Example with multiple attributes:
//
//  // Find all heroes with the same strength and speed, sum their intelligence
//  rows := r.Table("heroes").GroupBy([]string{"strength", "speed"}, r.Count()).Run(session)
func (e Exp) GroupBy(attribute, groupedMapReduce interface{}) Exp {
	_, ok := attribute.(string)
	if ok {
		attribute = List{attribute}
	}
	return naryOperator(groupByKind, e, attribute, groupedMapReduce)
}

// Contains returns true if all the specified values appear in the array, false
// otherwise.
//
// Example usage:
//
//  var response bool
//  err = r.Expr(r.List{"a", "b", "c"}).Contains("a", "b").Run(session).One(&response)
//
// Example response:
//
//  true
func (e Exp) Contains(values ...interface{}) Exp {
	return naryOperator(containsKind, e, values...)
}
