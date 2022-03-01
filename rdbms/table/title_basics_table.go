package table

import (
	"github.com/turnon/imdba/rdbms/asyncdb"
	"github.com/turnon/imdba/tsv"
)

type titleBasicsTable struct {
	insertStatements map[int]*string
}

func NewTitleBasicsTable() *titleBasicsTable {
	insertStatements := make(map[int]*string)
	return &titleBasicsTable{insertStatements: insertStatements}
}

func (tbs *titleBasicsTable) getInsertStatement(paramsCount int) *string {
	insertStatement, ok := tbs.insertStatements[paramsCount]
	if ok {
		return insertStatement
	}

	colums := []string{"id", "title_type", "primary_title", "original_title", "is_adult", "start_year", "end_year", "runtime_minutes"}
	concatedInsertStatement := generateInsertStmt("title_basics", colums, paramsCount)
	tbs.insertStatements[paramsCount] = &concatedInsertStatement
	return &concatedInsertStatement
}

func (tbs *titleBasicsTable) Insert(adb *asyncdb.AsyncDb, records ...*tsv.TitleBasicRow) error {
	recordCount := len(records)
	bindings := make([]interface{}, 0, recordCount*8)

	for _, r := range records {
		bindings = append(bindings, r.Id(), r.TitleType, r.PrimaryTitle, r.OriginalTitle, r.IsAdult, r.StartYear, r.EndYear, r.RuntimeMinutes)
	}

	insertStatement := tbs.getInsertStatement(recordCount)
	return adb.Exec(insertStatement, bindings)
}
