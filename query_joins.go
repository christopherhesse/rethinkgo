package rethinkgo

// InnerJoin performs an inner join on two sequences, using the provided
// function to compare the rows from each sequence. See also .EqJoin() and
// .OuterJoin().
//
// Each row from the left sequence is compared to every row from the right
// sequence using the provided predicate function.  If the function returns
// true for a pair of rows, that pair will appear in the resulting sequence.
//
// Example usage:
//
//  var response []interface{}
//  // Get each hero and their associated lair, in this case, "villain_id" is
//  // the primary key for the "lairs" table
//  compareRows := func (left, right r.Exp) r.Exp {
//      return left.Attr("id").Eq(right.Attr("villain_id"))
//  }
//  err := r.Table("villains").InnerJoin(r.Table("lairs"), compareRows).Run(session).All(&response)
//
// Example response:
//
//  [
//    {
//      "left": {
//        "durability": 6,
//        "energy": 6,
//        "fighting": 3,
//        "id": "c0d1b94f-b07e-40c3-a1db-448e645daedc",
//        "intelligence": 6,
//        "name": "Magneto",
//        "real_name": "Max Eisenhardt",
//        "speed": 4,
//        "strength": 2
//      },
//      "right": {
//        "lair": "Asteroid M",
//        "villain_id": "c0d1b94f-b07e-40c3-a1db-448e645daedc"
//      }
//    }
//  ]
func (leftExpr Exp) InnerJoin(rightExpr Exp, predicate interface{}) Exp {
	return naryOperator(innerJoinKind, leftExpr, rightExpr, funcWrapper(predicate, 2))
}

// OuterJoin performs a left outer join on two sequences, using the provided
// function to compare the rows from each sequence. See also .EqJoin() and
// .InnerJoin().
//
// Each row from the left sequence is compared to every row from the right
// sequence using the provided predicate function.  If the function returns
// true for a pair of rows, that pair will appear in the resulting sequence.
//
// If the predicate is false for every pairing for a specific left row, the left
// row will appear in the sequence with no right row present.
//
// Example usage:
//
//  var response []interface{}
//  // Get each hero and their associated lair, in this case, "villain_id" is
//  // the primary key for the "lairs" table
//  compareRows := func (left, right r.Exp) r.Exp {
//      return left.Attr("id").Eq(right.Attr("villain_id"))
//  }
//  err := r.Table("villains").OuterJoin(r.Table("lairs"), compareRows).Run(session).All(&response)
//
// Example response:
//
//  [
//    {
//      "left": {
//        "durability": 6,
//        "energy": 6,
//        "fighting": 3,
//        "id": "c0d1b94f-b07e-40c3-a1db-448e645daedc",
//        "intelligence": 6,
//        "name": "Magneto",
//        "real_name": "Max Eisenhardt",
//        "speed": 4,
//        "strength": 2
//      },
//      "right": {
//        "lair": "Asteroid M",
//        "villain_id": "c0d1b94f-b07e-40c3-a1db-448e645daedc"
//      }
//    },
//    {
//      "left": {
//        "durability": 4,
//        "energy": 1,
//        "fighting": 7,
//        "id": "ab140a9c-63d1-455e-862e-045ad7f57ae3",
//        "intelligence": 2,
//        "name": "Sabretooth",
//        "real_name": "Victor Creed",
//        "speed": 2,
//        "strength": 4
//      }
//    }
//    ...
//  ]
func (leftExpr Exp) OuterJoin(rightExpr Exp, predicate interface{}) Exp {
	return naryOperator(outerJoinKind, leftExpr, rightExpr, funcWrapper(predicate, 2))
}

// EqJoin performs a join on two expressions, it is more efficient than
// .InnerJoin() and .OuterJoin() because it looks up elements in the right table
// by primary key. See also .InnerJoin() and .OuterJoin().
//
// Example usage:
//
//  var response []interface{}
//  // Get each hero and their associated lair
//  query := r.Table("villains").EqJoin("id", r.Table("lairs"), "villain_id")
//  err := query.Run(session).All(&response)
//
// Example response:
//
//  [
//    {
//      "left": {
//        "durability": 6,
//        "energy": 6,
//        "fighting": 3,
//        "id": "c0d1b94f-b07e-40c3-a1db-448e645daedc",
//        "intelligence": 6,
//        "name": "Magneto",
//        "real_name": "Max Eisenhardt",
//        "speed": 4,
//        "strength": 2
//      },
//      "right": {
//        "lair": "Asteroid M",
//        "villain_id": "c0d1b94f-b07e-40c3-a1db-448e645daedc"
//      }
//    },
//    ...
//  ]
func (leftExpr Exp) EqJoin(leftAttribute string, rightExpr Exp, index string) Exp {
	return naryOperator(eqJoinKind, leftExpr, leftAttribute, rightExpr, index)
}

// Zip flattens the results of a join by merging the "left" and "right" fields
// of each row together.  If any keys conflict, the "right" field takes
// precedence.
//
// Example without .Zip():
//
//  var response []interface{}
//  // Find each hero-villain pair with the same strength
//  equalStrength := func(hero, villain r.Exp) r.Exp {
//      return hero.Attr("strength").Eq(villain.Attr("strength"))
//  }
//  query := r.Table("heroes").InnerJoin(r.Table("villains"), equalStrength)
//  err := query.Run(session).All(&response)
//
// Example response:
//
//  [
//    {
//      "left":
//      {
//        "durability": 5,
//        "energy": 7,
//        "fighting": 7,
//        "id": "f915d9a7-6cfa-4151-b5f6-6aded7da595f",
//        "intelligence": 5,
//        "name": "Nightcrawler",
//        "real_name": "Kurt Wagner",
//        "speed": 7,
//        "strength": 4
//      },
//      "right":
//      {
//        "durability": 4,
//        "energy": 1,
//        "fighting": 7,
//        "id": "12e58b11-93b3-4e89-987d-efb345001dfe",
//        "intelligence": 2,
//        "name": "Sabretooth",
//        "real_name": "Victor Creed",
//        "speed": 2,
//        "strength": 4
//      }
//    },
//    ...
//  ]
//
// Example with .Zip():
//
//  var response []interface{}
//  // Find each hero-villain pair with the same strength
//  equalStrength := func(hero, villain r.Exp) r.Exp {
//      return hero.Attr("strength").Eq(villain.Attr("strength"))
//  }
//  query := r.Table("heroes").InnerJoin(r.Table("villains"), equalStrength).Zip()
//  err := query.Run(session).All(&response)
//
// Example response:
//
//  [
//    {
//      "durability": 4,
//      "energy": 1,
//      "fighting": 7,
//      "id": "12e58b11-93b3-4e89-987d-efb345001dfe",
//      "intelligence": 2,
//      "name": "Sabretooth",
//      "real_name": "Victor Creed",
//      "speed": 2,
//      "strength": 4
//    },
//    ...
//  ]
func (e Exp) Zip() Exp {
	return naryOperator(zipKind, e)
}
