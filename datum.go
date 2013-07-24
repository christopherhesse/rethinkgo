package rethinkgo

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/json"
	"reflect"
	p "github.com/christopherhesse/rethinkgo/ql2"
	"strings"
)

func datumMarshal(v interface{}) (*p.Datum, error) {
	// convert arbitrary types to a datum tree using the json module
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var w interface{}
	err = json.Unmarshal(data, &w)
	if err != nil {
		return nil, err
	}
	return jsonToDatum(w), nil
}

func jsonToDatum(v interface{}) *p.Datum {
	value := reflect.ValueOf(v)
	switch value.Kind() {
	case reflect.Invalid:
		return &p.Datum{
			Type: p.Datum_R_NULL.Enum(),
		}
	case reflect.Bool:
		r := value.Bool()
		return &p.Datum{
			Type:  p.Datum_R_BOOL.Enum(),
			RBool: &r,
		}
	case reflect.Float64:
		r := value.Float()
		return &p.Datum{
			Type: p.Datum_R_NUM.Enum(),
			RNum: &r,
		}
	case reflect.String:
		r := value.String()
		return &p.Datum{
			Type: p.Datum_R_STR.Enum(),
			RStr: &r,
		}
	case reflect.Slice:
		datums := []*p.Datum{}
		for i := 0; i < value.Len(); i++ {
			itemValue := value.Index(i)
			datums = append(datums, jsonToDatum(itemValue.Interface()))
		}

		return &p.Datum{
			Type:   p.Datum_R_ARRAY.Enum(),
			RArray: datums,
		}
	case reflect.Map:
		pairs := []*p.Datum_AssocPair{}
		for _, keyValue := range value.MapKeys() {
			valueValue := value.MapIndex(keyValue)
			// keys for objects must be strings
			key := keyValue.Interface().(string)
			pair := &p.Datum_AssocPair{
				Key: proto.String(key),
				Val: jsonToDatum(valueValue.Interface()),
			}
			pairs = append(pairs, pair)
		}
		return &p.Datum{
			Type:    p.Datum_R_OBJECT.Enum(),
			RObject: pairs,
		}
	}
	panic("rethinkdb: could not convert to datum")
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
