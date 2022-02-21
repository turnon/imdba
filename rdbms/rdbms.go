package rdbms

import (
	"database/sql"
	"strings"

	"github.com/turnon/imdba/tsv"
)

type SqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

func InsertTitleBasics(executor *sql.DB, records ...*tsv.TitleBasicRow) (err error) {
	insertIntoValues := "INSERT INTO title_basics (id, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes, genres) VALUES "
	valuesStatement := "(?, ?, ?, ?, ?, ?, ?, ?, ?)"

	recordCount := len(records)
	params := make([]string, 0, recordCount)
	bindings := make([]interface{}, 0, recordCount*9)

	for _, r := range records {
		params = append(params, valuesStatement)
		bindings = append(bindings, r.Id())
		bindings = append(bindings, r.TitleType)
		bindings = append(bindings, r.PrimaryTitle)
		bindings = append(bindings, r.OriginalTitle)
		bindings = append(bindings, r.IsAdult)
		bindings = append(bindings, r.StartYear)
		bindings = append(bindings, r.EndYear)
		bindings = append(bindings, r.RuntimeMinutes)
		bindings = append(bindings, r.Genres)
	}

	insertStatement := insertIntoValues + strings.Join(params, ",")
	_, err = executor.Exec(insertStatement, bindings...)
	if err != nil {
		return
	}

	return
}
