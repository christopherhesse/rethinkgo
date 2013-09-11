package time

import (
	"encoding/json"
	"errors"
	"time"
)

type Time struct {
	time.Time
}

func (t Time) MarshalJSON() ([]byte, error) {
	if y := t.Year(); y < 0 || y >= 10000 {
		return nil, errors.New("Time.MarshalJSON: year outside of range [0,9999]")
	}

	return json.Marshal(map[string]interface{}{
		"$reql_type$": "TIME",
		"epoch_time":  t.Unix(),
		"timezone":    "+00:00",
	})
}
