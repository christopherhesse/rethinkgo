package rethinkgo

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

// Update: updated, skipped, errors, first_error
// Replace: deleted, errors, inserted, modified, first_error
