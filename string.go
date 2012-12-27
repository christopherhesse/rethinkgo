package rethinkgo

// Convert Expression, WriteQuery, and MetaQuery objects to strings
func (e Expression) String() string {
	switch e.kind {
	case variableKind:
		// this needs to be just the variable name so that users can create
		// javascript expressions within functions.
		return e.value.(string)
	}
	return "some sort of expression"
}
