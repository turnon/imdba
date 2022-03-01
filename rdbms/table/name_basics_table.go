package table

import (
	"github.com/turnon/imdba/rdbms/asyncdb"
	"github.com/turnon/imdba/tsv"
)

type nameBasicsTable struct {
	insertStatements map[int]*string
}

func NewNameBasicsTable() *nameBasicsTable {
	insertStatements := make(map[int]*string)
	return &nameBasicsTable{insertStatements: insertStatements}
}

func (nbs *nameBasicsTable) getInsertStatement(paramsCount int) *string {
	insertStatement, ok := nbs.insertStatements[paramsCount]
	if ok {
		return insertStatement
	}

	concatInsertStatement := generateInsertStmt("name_basics", []string{"id", "primary_name", "birth_year", "death_year"}, paramsCount)
	nbs.insertStatements[paramsCount] = &concatInsertStatement
	return &concatInsertStatement
}

func (nbs *nameBasicsTable) Insert(adb *asyncdb.AsyncDb, records ...*tsv.NameBasicRow) error {
	recordCount := len(records)
	bindings := make([]interface{}, 0, recordCount*4)

	for _, r := range records {
		bindings = append(bindings, r.Id(), r.PrimaryName, r.BirthYear, r.DeathYear)
	}

	insertStatement := nbs.getInsertStatement(recordCount)
	return adb.Exec(insertStatement, bindings)
}
