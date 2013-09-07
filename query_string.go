package rethinkgo

// Match matches a regular expression against a string.  The regular expression
// syntax is RE2, which is the same used by the "regexp" package.
//
// Example usage:
//
//  var response interface{}
//  err = r.Expr("3.14159").Match("[0-9]+").Run(session).One(&response)
//
// Example response:
//
//  {"str": "3", "start": 0, "end": 1, "groups": []}
func (e Exp) Match(regularExpression string) Exp {
	return naryOperator(matchKind, e, regularExpression)
}
