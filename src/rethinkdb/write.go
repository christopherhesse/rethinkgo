// All write queries

package rethinkdb

import (
	"code.google.com/p/goprotobuf/proto"
	"errors"
	p "rethinkdb/query_language"
)

func buildWriteQuery(queryType p.WriteQuery_WriteQueryType) *p.Query {
	return &p.Query{
		Type: p.Query_WRITE.Enum(),
		WriteQuery: &p.WriteQuery{
			Type: queryType.Enum(),
		},
	}
}

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

type InsertQuery struct {
	tableExpr Expression
	rows      []interface{}
	overwrite bool
}

func (e Expression) Insert(rows ...interface{}) InsertQuery {
	// Assume the expression is a table for now, we'll check later in buildProtobuf
	return InsertQuery{
		tableExpr: e,
		rows:      rows,
		overwrite: false,
	}
}

func (q InsertQuery) Overwrite(overwrite bool) InsertQuery {
	q.overwrite = overwrite
	return q
}

func (q InsertQuery) buildProtobuf() (query *p.Query, err error) {
	var terms []*p.Term
	for _, row := range q.rows {
		term, err := buildTerm(row)
		if err != nil {
			return nil, err
		}
		terms = append(terms, term)
	}

	table, ok := q.tableExpr.value.(TableInfo)
	if !ok {
		err = errors.New("rethinkdb: Inserts can only be performed on tables :(")
		return
	}

	query = buildWriteQuery(p.WriteQuery_INSERT)

	query.WriteQuery.Insert = &p.WriteQuery_Insert{
		TableRef:  table.toTableRef(),
		Terms:     terms,
		Overwrite: proto.Bool(q.overwrite),
	}
	return
}

type UpdateQuery struct {
	view    Expression
	mapping interface{}
}

func (e Expression) Update(mapping interface{}) UpdateQuery {
	return UpdateQuery{
		view:    e,
		mapping: mapping,
	}
}

func (q UpdateQuery) buildProtobuf() (query *p.Query, err error) {
	view, err := buildTerm(q.view)
	if err != nil {
		return
	}

	mapping, err := buildMapping(q.mapping)
	if err != nil {
		return
	}

	if view.GetType() == p.Term_GETBYKEY {
		// this is chained off of a .Get(), do a POINTUPDATE
		query = buildWriteQuery(p.WriteQuery_POINTUPDATE)

		query.WriteQuery.PointUpdate = &p.WriteQuery_PointUpdate{
			TableRef: view.GetByKey.TableRef,
			Attrname: view.GetByKey.Attrname,
			Key:      view.GetByKey.Key,
			Mapping:  mapping,
		}
		return
	}

	query = buildWriteQuery(p.WriteQuery_UPDATE)

	query.WriteQuery.Update = &p.WriteQuery_Update{
		View:    view,
		Mapping: mapping,
	}
	return
}

type ReplaceQuery struct {
	view    Expression
	mapping interface{}
}

func (e Expression) Replace(mapping interface{}) ReplaceQuery {
	return ReplaceQuery{
		view:    e,
		mapping: mapping,
	}
}

func (q ReplaceQuery) buildProtobuf() (query *p.Query, err error) {
	view, err := buildTerm(q.view)
	if err != nil {
		return
	}

	mapping, err := buildMapping(q.mapping)
	if err != nil {
		return
	}

	if view.GetType() == p.Term_GETBYKEY {
		query = buildWriteQuery(p.WriteQuery_POINTMUTATE)

		query.WriteQuery.PointMutate = &p.WriteQuery_PointMutate{
			TableRef: view.GetByKey.TableRef,
			Attrname: view.GetByKey.Attrname,
			Key:      view.GetByKey.Key,
			Mapping:  mapping,
		}
		return
	}

	query = buildWriteQuery(p.WriteQuery_MUTATE)

	query.WriteQuery.Mutate = &p.WriteQuery_Mutate{
		View:    view,
		Mapping: mapping,
	}
	return
}

type DeleteQuery struct {
	view Expression
}

func (e Expression) Delete() DeleteQuery {
	return DeleteQuery{view: e}
}

func (q DeleteQuery) buildProtobuf() (query *p.Query, err error) {
	view, err := buildTerm(q.view)
	if err != nil {
		return
	}

	if view.GetType() == p.Term_GETBYKEY {
		query = buildWriteQuery(p.WriteQuery_POINTDELETE)

		query.WriteQuery.PointDelete = &p.WriteQuery_PointDelete{
			TableRef: view.GetByKey.TableRef,
			Attrname: view.GetByKey.Attrname,
			Key:      view.GetByKey.Key,
		}
		return
	}

	query = buildWriteQuery(p.WriteQuery_DELETE)

	query.WriteQuery.Delete = &p.WriteQuery_Delete{
		View: view,
	}
	return
}

type ForEachQuery struct {
	stream    Expression
	queryFunc func(Expression) RethinkQuery
}

func (e Expression) ForEach(queryFunc (func(Expression) RethinkQuery)) ForEachQuery {
	return ForEachQuery{stream: e, queryFunc: queryFunc}
}

func (q ForEachQuery) buildProtobuf() (query *p.Query, err error) {
	stream, err := buildTerm(q.stream)
	if err != nil {
		return
	}

	name := nextVariableName()
	generatedQuery := q.queryFunc(LetVar(name))
	innerQuery, err := generatedQuery.buildProtobuf()
	if err != nil {
		return
	}

	query = buildWriteQuery(p.WriteQuery_FOREACH)

	query.WriteQuery.ForEach = &p.WriteQuery_ForEach{
		Stream:  stream,
		Var:     proto.String(name),
		Queries: []*p.WriteQuery{innerQuery.WriteQuery},
	}
	return
}
