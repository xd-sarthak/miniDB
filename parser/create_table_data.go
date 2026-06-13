package parser

import "github.com/xd-sarthak/miniDB/records"

type CreateTableData struct {
	tableName string
	schema    *records.Schema
}

func NewCreateTableData(tableName string, sch *records.Schema) *CreateTableData {
	return &CreateTableData{
		tableName: tableName,
		schema:    sch,
	}
}

func (ctd *CreateTableData) TableName() string {
	return ctd.tableName
}

func (ctd *CreateTableData) NewSchema() *records.Schema {
	return ctd.schema
}
