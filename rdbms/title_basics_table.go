package rdbms

import (
	"strings"

	"github.com/turnon/imdba/tsv"
)

type titleBasicsTable struct {
	insertStatements map[int]*string
}

func newTitleBasicsTable() *titleBasicsTable {
	insertStatements := make(map[int]*string)
	return &titleBasicsTable{insertStatements: insertStatements}
}

func (tbs *titleBasicsTable) getInsertStatement(paramsCount int) *string {
	insertStatement, ok := tbs.insertStatements[paramsCount]
	if ok {
		return insertStatement
	}

	insertIntoValues := "INSERT INTO title_basics (id, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes) VALUES "
	valuesStatement := "(?, ?, ?, ?, ?, ?, ?, ?)"
	valuesStatements := make([]string, 0, paramsCount)
	onConflict := " ON CONFLICT DO NOTHING"

	for ; paramsCount > 0; paramsCount -= 1 {
		valuesStatements = append(valuesStatements, valuesStatement)
	}

	originalInsertStatement := insertIntoValues + strings.Join(valuesStatements, ",") + onConflict
	tbs.insertStatements[paramsCount] = &originalInsertStatement
	return &originalInsertStatement
}

func (tbs *titleBasicsTable) insert(executor *asyncDb, records ...*tsv.TitleBasicRow) error {
	recordCount := len(records)
	bindings := make([]interface{}, 0, recordCount*8)

	for _, r := range records {
		bindings = append(bindings, r.Id(), r.TitleType, r.PrimaryTitle, r.OriginalTitle, r.IsAdult, r.StartYear, r.EndYear, r.RuntimeMinutes)
	}

	insertStatement := tbs.getInsertStatement(recordCount)
	executor.exec(insertStatement, bindings)

	return nil
}
