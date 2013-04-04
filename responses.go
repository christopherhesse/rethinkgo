package rethinkgo

// WriteResponse is a type that can be used to read responses to write queries, such as .Insert()
//
// Example usage:
//
//  var response r.WriteResponse
//  err := r.Table("heroes").Insert(r.Map{"name": "Professor X"}).Run(session).One(&response)
//  fmt.Println("inserted", response.Inserted, "rows")
type WriteResponse struct {
	Inserted      int
	Errors        int
	Updated       int
	Unchanged     int
	Replaced      int
	Deleted       int
	GeneratedKeys []string `json:"generated_keys"`
	FirstError    string   `json:"first_error"` // populated if Errors > 0
}
