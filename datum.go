package rethinkgo

import (
	"encoding/json"
	p "github.com/christopherhesse/rethinkgo/ql2"
	"strconv"
	"strings"
	"time"
)

func datumMarshal(v interface{}) (*p.Term, error) {
	// convert arbitrary types to a datum tree using the json module
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	dataString := string(data)

	datumTerm := &p.Term{
		Type: p.Term_DATUM.Enum(),
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
		obj := map[string]string{}

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

			obj[string(key)] = string(val)
		}

		for key, val := range obj {
			if key == "\"$reql_type$\"" {
				if val == "\"TIME\"" {
					epochTime := obj["\"epoch_time\""]
					timezone, err := strconv.Unquote(obj["\"timezone\""])
					if err != nil {
						return nil, err
					}

					seconds, err := strconv.ParseFloat(epochTime, 64)
					if err != nil {
						return nil, err
					}
					t := time.Unix(int64(seconds), 0)

					// Caclulate the timezone
					if timezone != "" {
						hours, err := strconv.Atoi(timezone[1:3])
						if err != nil {
							return nil, err
						}
						minutes, err := strconv.Atoi(timezone[4:6])
						if err != nil {
							return nil, err
						}
						tzOffset := ((hours * 60) + minutes) * 60
						if timezone[:1] == "-" {
							tzOffset = 0 - tzOffset
						}

						t = t.In(time.FixedZone(timezone, tzOffset))
					}

					b, err := json.Marshal(t)
					if err != nil {
						return nil, err
					}
					return b, nil
				} else {
					panic("unknown pseudo-type")
				}
			}

			pairs = append(pairs, key+":"+val)
		}

		return []byte("{" + strings.Join(pairs, ",") + "}"), nil
	}
	panic("unknown datum type")
}
