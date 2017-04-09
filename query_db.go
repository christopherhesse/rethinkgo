package rethinkgo

// DbCreate creates a database with the supplied name.
//
// Example usage:
//
//  err := r.DbCreate("marvel").Run(session).Exec()
func DbCreate(name string) Exp {
	return naryOperator(databaseCreateKind, name)
}

// DbDrop deletes the specified database
//
// Example usage:
//
//  err := r.DbDrop("marvel").Run(session).Exec()
func DbDrop(name string) Exp {
	return naryOperator(databaseDropKind, name)
}

// DbList lists all databases on the server
//
// Example usage:
//
//  var databases []string
//  err := r.DbList().Run(session).All(&databases)
//
// Example response:
//
//  ["test", "marvel"]
func DbList() Exp {
	return Exp{kind: databaseListKind}
}
