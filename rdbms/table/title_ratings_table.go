package table

import (
	"github.com/turnon/imdba/rdbms/asyncdb"
	"github.com/turnon/imdba/tsv"
)

type titleRatingsTable struct {
	insertStatements map[int]*string
}

func NewTitleRatingsTable() *titleRatingsTable {
	insertStatements := make(map[int]*string)
	return &titleRatingsTable{insertStatements: insertStatements}
}

func (tbs *titleRatingsTable) getInsertStatement(paramsCount int) *string {
	insertStatement, ok := tbs.insertStatements[paramsCount]
	if ok {
		return insertStatement
	}

	concatedInsertStatement := generateInsertStmt("title_ratings", []string{"id", "rating", "votes"}, paramsCount) + " ON CONFLICT DO NOTHING"
	tbs.insertStatements[paramsCount] = &concatedInsertStatement
	return &concatedInsertStatement
}

func (trs *titleRatingsTable) Insert(adb *asyncdb.AsyncDb, records ...*tsv.TitleRatingRow) error {
	recordCount := len(records)
	bindings := make([]interface{}, 0, recordCount*3)

	for _, r := range records {
		bindings = append(bindings, r.TtId(), r.AvgRatingInt(), r.NumVotesInt())
	}

	insertStatement := trs.getInsertStatement(recordCount)
	return adb.Exec(insertStatement, bindings)
}
