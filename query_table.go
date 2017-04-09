package rethinkgo

// TableSpec lets you specify the various parameters for a table, then create it
// with TableCreateWithSpec().  See that function for documentation.
type TableSpec struct {
	Name       string
	PrimaryKey string
	Datacenter string
	CacheSize  int64
	Durability string // either "soft" or "hard"
}

// TableCreate creates a table with the specified name.
//
// Example usage:
//
//  err := r.TableCreate("heroes").Run(session).Exec()
func TableCreate(name string) Exp {
	spec := TableSpec{Name: name}
	return naryOperator(tableCreateKind, spec)
}

func (e Exp) TableCreate(name string) Exp {
	spec := TableSpec{Name: name}
	return naryOperator(tableCreateKind, e, spec)
}

// TableCreateWithSpec creates a table with the specified attributes.
//
// Example usage:
//
//  spec := TableSpec{Name: "heroes", PrimaryKey: "name"}
//  err := r.TableCreateWithSpec(spec).Run(session).Exec()
func TableCreateWithSpec(spec TableSpec) Exp {
	return naryOperator(tableCreateKind, spec)
}

func (e Exp) TableCreateWithSpec(spec TableSpec) Exp {
	return naryOperator(tableCreateKind, e, spec)
}

// TableList lists all tables in the database.
//
// Example usage:
//
//  var tables []string
//  err := r.TableList().Run(session).All(&tables)
//
// Example response:
//
//  ["heroes", "villains"]
func TableList() Exp {
	return nullaryOperator(tableListKind)
}

func (e Exp) TableList() Exp {
	return naryOperator(tableListKind, e)
}

// TableDrop removes a table from the database.
//
// Example usage:
//
//  err := r.Db("marvel").TableDrop("heroes").Run(session).Exec()
func TableDrop(name string) Exp {
	return naryOperator(tableDropKind, name)
}

func (e Exp) TableDrop(name string) Exp {
	return naryOperator(tableDropKind, e, name)
}

// IndexCreate creates a secondary index on the specified table with the given
// name.  If the function for the index is nil, the index is created for an attribute
// with the same name as the index.
//
// Example usage:
//
//  var response map[string]int
//  err := r.Table("heroes").IndexCreate("name", nil).Run(session).All(&response)
//
// Example response:
//
//  {
//    "created": 1,
//  }
//
// Example usage with function:
//
//  var response map[string]int
//  awesomeness_f := func(hero r.Exp) r.Exp {
//    return hero.Attr("speed").Mul(hero.Attr("strength"))
//  }
//  err := r.Table("heroes").IndexCreate("name", awesomeness_f).Run(session).All(&response)
//
// Example response:
//
//  {
//    "created": 1,
//  }
func (e Exp) IndexCreate(name string, function interface{}) Exp {
	if function == nil {
		return naryOperator(indexCreateKind, e, name)
	}
	return naryOperator(indexCreateKind, e, name, funcWrapper(function, 1))
}

// IndexList lists all secondary indexes on a specified table.
//
// Example usage:
//
//  var response []string
//  err := r.Table("heroes").IndexList().Run(session).One(&response)
//
// Example response:
//
//  ["name", "speed"]
func (e Exp) IndexList() Exp {
	return naryOperator(indexListKind, e)
}

// IndexDrop deletes a secondary index from a table.
//
// Example usage:
//
//  var response map[string]int
//  err := r.Table("heroes").IndexDrop("name").Run(session).One(&response)
//
// Example response:
//
//  {
//    "dropped": 1,
//  }
func (e Exp) IndexDrop(name string) Exp {
	return naryOperator(indexDropKind, e, name)
}
