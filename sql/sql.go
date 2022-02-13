package sql

import (
	"database/sql"

	"github.com/turnon/imdba/tsv"
)

type SqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

func InsertTitleBasics(executor SqlExecutor, records ...*tsv.TitleBasicRow) (err error) {
	const insert_statement = "INSERT INTO " +
		"title_basics (id, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes, genres) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

	for _, r := range records {
		_, err = executor.Exec(insert_statement, r.Id(), r.TitleType, r.PrimaryTitle, r.OriginalTitle, r.IsAdult, r.StartYear, r.EndYear, r.RuntimeMinutes, r.Genres)
		if err != nil {
			return
		}
	}

	return
}
