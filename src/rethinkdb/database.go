package rethinkdb

import (
	"code.google.com/p/goprotobuf/proto"
	p "rethinkdb/query_language"
)

func MetaQuery(queryType p.MetaQuery_MetaQueryType) *p.Query {
	return &p.Query{
		Type: p.Query_META.Enum(),
		MetaQuery: &p.MetaQuery{
			Type: queryType.Enum(),
		},
	}
}

type CreateDatabaseQuery struct {
	name string
}

// Create a database
func DBCreate(name string) CreateDatabaseQuery {
	return CreateDatabaseQuery{name}
}

func (q CreateDatabaseQuery) buildProtobuf() (*p.Query, error) {
	query := MetaQuery(p.MetaQuery_CREATE_DB)
	query.MetaQuery.DbName = proto.String(q.name)
	return query, nil
}

type DropDatabaseQuery struct {
	name string
}

// Drop database
func DBDrop(name string) DropDatabaseQuery {
	return DropDatabaseQuery{name}
}

func (q DropDatabaseQuery) buildProtobuf() (*p.Query, error) {
	query := MetaQuery(p.MetaQuery_DROP_DB)
	query.MetaQuery.DbName = proto.String(q.name)
	return query, nil
}

type ListDatabasesQuery struct {
}

// List all databases
func DBList() ListDatabasesQuery {
	return ListDatabasesQuery{}
}

func (q ListDatabasesQuery) buildProtobuf() (*p.Query, error) {
	return MetaQuery(p.MetaQuery_LIST_DBS), nil
}

type Database struct {
	name string
}

func DB(name string) Database {
	return Database{name}
}

type TableCreateQuery struct {
	name     string
	database Database

	// These can be set by the user
	PrimaryKey        string
	PrimaryDatacenter string
	CacheSize         int64
}

func (db Database) TableCreate(name string) TableCreateQuery {
	return TableCreateQuery{name: name, database: db}
}

func (q TableCreateQuery) buildProtobuf() (query *p.Query, err error) {
	query = MetaQuery(p.MetaQuery_CREATE_TABLE)
	query.MetaQuery.CreateTable = &p.MetaQuery_CreateTable{
		PrimaryKey: protoStringOrNil(q.PrimaryKey),
		Datacenter: protoStringOrNil(q.PrimaryDatacenter),
		TableRef: &p.TableRef{
			DbName:    proto.String(q.database.name),
			TableName: proto.String(q.name),
		},
		CacheSize: protoInt64OrNil(q.CacheSize),
	}
	return
}

type TableListQuery struct {
	database Database
}

// List all tables in this database
func (db Database) TableList() TableListQuery {
	return TableListQuery{db}
}

func (q TableListQuery) buildProtobuf() (*p.Query, error) {
	query := MetaQuery(p.MetaQuery_LIST_TABLES)
	query.MetaQuery.DbName = proto.String(q.database.name)
	return query, nil
}

type TableDropQuery struct {
	name     string
	database Database
}

// Drop a table from a database
func (db Database) TableDrop(name string) TableDropQuery {
	return TableDropQuery{name: name, database: db}
}

func (q TableDropQuery) buildProtobuf() (*p.Query, error) {
	query := MetaQuery(p.MetaQuery_DROP_TABLE)
	query.MetaQuery.DropTable = &p.TableRef{
		TableName: proto.String(q.name),
		DbName:    proto.String(q.database.name),
	}
	return query, nil
}

type TableInfo struct {
	name        string
	database    Database
	useOutdated bool
}

func (db Database) Table(name string) Expression {
	value := TableInfo{
		name:        name,
		database:    db,
		useOutdated: false,
	}
	return Expression{kind: TableKind, value: value}
}

func (e Expression) UseOutdated(useOutdated bool) Expression {
	// TODO: this will cause an uncatchable runtime error if used incorrectly,
	// is there an alternative? TableUseOutdated(table), defer error until runtime?
	// OutdatedTableKind that uses the TableKind expression as a value
	// and fails on .Term()
	t := e.value.(TableInfo)
	t.useOutdated = useOutdated
	return Expression{kind: TableKind, value: t}
}

func (table TableInfo) toTableRef() *p.TableRef {
	return &p.TableRef{
		TableName:   proto.String(table.name),
		DbName:      proto.String(table.database.name),
		UseOutdated: proto.Bool(table.useOutdated),
	}
}
