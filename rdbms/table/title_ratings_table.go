package table

import (
	"strings"

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

	insertIntoValues := "INSERT INTO title_ratings (id, rating, votes) VALUES "
	valuesStatement := "(?, ?, ?)"
	valuesStatements := make([]string, 0, paramsCount)
	onConflict := " ON CONFLICT DO NOTHING"

	for ; paramsCount > 0; paramsCount -= 1 {
		valuesStatements = append(valuesStatements, valuesStatement)
	}

	originalInsertStatement := insertIntoValues + strings.Join(valuesStatements, ",") + onConflict
	tbs.insertStatements[paramsCount] = &originalInsertStatement
	return &originalInsertStatement
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
