package table

import (
	"strings"

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

	insertIntoValues := "INSERT INTO name_basics (id, primary_name, birth_year, death_year) VALUES "
	valuesStatement := "(?, ?, ?, ?)"
	valuesStatements := make([]string, 0, paramsCount)
	onConflict := " ON CONFLICT DO NOTHING"

	for ; paramsCount > 0; paramsCount -= 1 {
		valuesStatements = append(valuesStatements, valuesStatement)
	}

	originalInsertStatement := insertIntoValues + strings.Join(valuesStatements, ",") + onConflict
	nbs.insertStatements[paramsCount] = &originalInsertStatement
	return &originalInsertStatement
}

func (nbs *nameBasicsTable) Insert(executor *asyncdb.AsyncDb, records ...*tsv.NameBasicRow) error {
	recordCount := len(records)
	bindings := make([]interface{}, 0, recordCount*4)

	for _, r := range records {
		bindings = append(bindings, r.Id(), r.PrimaryName, r.BirthYear, r.DeathYear)
	}

	insertStatement := nbs.getInsertStatement(recordCount)
	executor.Exec(insertStatement, bindings)

	return nil
}
