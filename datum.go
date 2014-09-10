package rethinkgo

import (
	"encoding/json"
	p "github.com/christopherhesse/rethinkgo/ql2"
	"strings"
)

func datumMarshal(v interface{}) (*p.Term, error) {
	// convert arbitrary types to a datum tree using the json module
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	dataString := string(data)

	datumTerm := &p.Term{
		Type:  p.Term_DATUM.Enum(),
		Datum: &p.Datum{
			Type: p.Datum_R_STR.Enum(),
			RStr: &dataString,
		},
	}

	term := &p.Term{
		Type: p.Term_JSON.Enum(),
		Args: []*p.Term{datumTerm},
	}

	return term, nil
}

func datumUnmarshal(datum *p.Datum, v interface{}) error {
	// convert a datum tree into an arbitrary type using the json module
	data, err := datumToJson(datum)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func isNullDatum(datum *p.Datum) bool {
	return datum.GetType() == p.Datum_R_NULL
}

func datumToJson(datum *p.Datum) ([]byte, error) {
	switch datum.GetType() {
	case p.Datum_R_NULL:
		return json.Marshal(nil)
	case p.Datum_R_BOOL:
		return json.Marshal(datum.GetRBool())
	case p.Datum_R_NUM:
		return json.Marshal(datum.GetRNum())
	case p.Datum_R_STR:
		return json.Marshal(datum.GetRStr())
	case p.Datum_R_ARRAY:
		items := []string{}
		for _, d := range datum.GetRArray() {
			item, err := datumToJson(d)
			if err != nil {
				return nil, err
			}
			items = append(items, string(item))
		}
		return []byte("[" + strings.Join(items, ",") + "]"), nil
	case p.Datum_R_OBJECT:
		pairs := []string{}
		for _, assoc := range datum.GetRObject() {
			raw_key := assoc.GetKey()
			raw_val := assoc.GetVal()

			// convert to json form
			key, err := json.Marshal(raw_key)
			if err != nil {
				return nil, err
			}
			val, err := datumToJson(raw_val)
			if err != nil {
				return nil, err
			}

			pairs = append(pairs, string(key)+":"+string(val))
		}
		return []byte("{" + strings.Join(pairs, ",") + "}"), nil
	}
	panic("unknown datum type")
}
