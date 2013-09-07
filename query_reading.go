package rethinkgo

// Db lets you perform operations within a specific database (this will override
// the database specified on the session).  This can be used to access or
// create/list/delete tables within any database available on the server.
//
// Example usage:
//
//  var response []interface{}
//  // this query will use the default database of the last created session
//  r.Table("test").Run(session).All(&response)
//  // this query will use database "marvel" regardless of what database the session has set
//  r.Db("marvel").Table("heroes").Run(session).All(&response)
func Db(name string) Exp {
	return naryOperator(databaseKind, name)
}

// Table references all rows in a specific table, using the database that this
// method was called on.
//
// Example usage:
//
//  var response []map[string]interface{}
//  err := r.Db("marvel").Table("heroes").Run(session).All(&response)
//
// Example response:
//
//  [
//    {
//      "strength": 3,
//      "name": "Doctor Strange",
//      "durability": 6,
//      "intelligence": 4,
//      "energy": 7,
//      "fighting": 7,
//      "real_name": "Stephen Vincent Strange",
//      "speed": 5,
//      "id": "edc3a46b-95a0-4f64-9d1c-0dd7d83c4bcd"
//    },
//    ...
//  ]
func Table(name string) Exp {
	return naryOperator(tableKind, name)
}

func (e Exp) Table(name string) Exp {
	return naryOperator(tableKind, e, name)
}

// Get retrieves a single row by primary key.
//
// Example usage:
//
//  var response map[string]interface{}
//  err := r.Table("heroes").Get("Doctor Strange").Run(session).One(&response)
//
// Example response:
//
//  {
//    "strength": 3,
//    "name": "Doctor Strange",
//    "durability": 6,
//    "intelligence": 4,
//    "energy": 7,
//    "fighting": 7,
//    "real_name": "Stephen Vincent Strange",
//    "speed": 5,
//    "id": "edc3a46b-95a0-4f64-9d1c-0dd7d83c4bcd"
//  }
func (e Exp) Get(key interface{}) Exp {
	return naryOperator(getKind, e, key)
}

// GetAll retrieves all documents where the given value matches the requested
// index.
//
// Example usage (awesomeness is a secondary index defined as speed * strength):
//
//  var response []interface{}
//  err := r.Table("heroes").GetAll("awesomeness", 10).Run(session).All(&response)
//
// Example response:
//
//  {
//    "strength": 2,
//    "name": "Storm",
//    "durability": 3,
//    "intelligence": 5,
//    "energy": 6,
//    "fighting": 5,
//    "real_name": "Ororo Munroe",
//    "speed": 5,
//    "id": "59d1ad55-a61e-49d9-a375-0fb014b0e6ea"
//  }
func (e Exp) GetAll(index string, values ...interface{}) Exp {
	return naryOperator(getAllKind, e, append(values, index)...)
}

// Between gets all rows where the key attribute's value falls between the
// lowerbound and upperbound (inclusive).  Use nil to represent no upper or
// lower bound.  Requires an index on the key (primary keys already have an
// index with the name of the primary key).
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve all heroes with names between "E" and "F"
//   err := r.Table("heroes").Between("name", "E", "F").Run(session).All(&response)
//
// Example response:
//
//  {
//    "strength": 4,
//    "name": "Elektra",
//    "durability": 2,
//    "intelligence": 4,
//    "energy": 3,
//    "fighting": 7,
//    "real_name": "Elektra Natchios",
//    "speed": 6,
//  }
func (e Exp) Between(index string, lowerKey, upperKey interface{}) Exp {
	return naryOperator(betweenKind, e, lowerKey, upperKey, index)
}

// LeftBound tells the server when performing a between to include the left endpoint
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve all heroes with names between "E" and "F"
//   err := r.Table("heroes").Between("name", "E", "F").RightBound.Run(session).All(&response)
//
func (e Exp) LeftBound(opt string) Exp {
	return naryOperator(leftboundKind, e, opt)
}

// RightBound tells the server when performing a between to include the right endpoint
//
// Example usage:
//
//   var response []interface{}
//   // Retrieve all heroes with names between "E" and "F"
//   err := r.Table("heroes").Between("name", "E", "F").RightBound.Run(session).All(&response)
//
func (e Exp) RightBound(opt string) Exp {
	return naryOperator(rightboundKind, e, opt)
}

// Filter removes all objects from a sequence that do not match the given
// condition.  The condition can be an RQL expression, an r.Map, or a function
// that returns true or false.
//
// Example with an RQL expression:
//
//   var response []interface{}
//   // Get all heroes with durability 6
//   err := r.Table("heroes").Filter(r.Row.Attr("durability").Eq(6)).Run(session).All(&response)
//
// Example with r.Map:
//
//   err := r.Table("heroes").Filter(r.Map{"durability": 6}).Run(session).All(&response)
//
// Example with function:
//
//   filterFunc := func (row r.Exp) r.Exp { return row.Attr("durability").Eq(6) }
//   err := r.Table("heroes").Filter(filterFunc).Run(session).All(&response)
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
//    }
//    ...
//  ]
func (e Exp) Filter(operand interface{}) Exp {
	return naryOperator(filterKind, e, funcWrapper(operand, 1))
}
