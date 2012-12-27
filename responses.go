package rethinkgo

// WriteResponse is a type that can be used to read any responses to write queries, such as .Insert()
//
// Example usage:
//
//  var response r.WriteResponse
//  err := r.Table("coffee").Insert(r.Map{"a":1}).Run().One(&response)
type WriteResponse struct {
	Inserted      int
	Errors        int
	Updated       int
	Skipped       int
	Modified      int
	Deleted       int
	GeneratedKeys []string `json:"generated_keys"`
	FirstError    string   `json:"first_error"` // populated if Errors > 0
}
