package rdbms

import (
	"database/sql"
	"strings"

	"github.com/turnon/imdba/tsv"
)

type genresHandler struct {
	genreIds map[string]int64
}

func newGenresHandler() *genresHandler {
	genreIds := make(map[string]int64)
	return &genresHandler{genreIds}
}

func (gh *genresHandler) mapTitleGenres(db *sql.DB, records ...*tsv.TitleBasicRow) error {
	insertIntoValues := "INSERT INTO title_genres (title_id, genre_id) VALUES "
	valuesStatement := "(?, ?)"
	valuesStatements := []string{}
	mapping := []interface{}{}

	for _, r := range records {
		for _, genre := range r.GenresArray() {
			gid, ok := gh.genreIds[genre]
			if !ok {
				result, err := db.Exec("INSERT INTO genres (genre) VALUES (?)", genre)
				if err != nil {
					return err
				}
				gid, _ = result.LastInsertId()
				gh.genreIds[genre] = gid
			}
			mapping = append(mapping, r.Id(), gid)
			valuesStatements = append(valuesStatements, valuesStatement)
		}
	}

	insertStatement := insertIntoValues + strings.Join(valuesStatements, ",")
	_, err := db.Exec(insertStatement, mapping...)
	if err != nil {
		return err
	}
	return nil
}

func InsertTitleBasics(executor *sql.DB, records ...*tsv.TitleBasicRow) error {
	insertIntoValues := "INSERT INTO title_basics (id, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes) VALUES "
	valuesStatement := "(?, ?, ?, ?, ?, ?, ?, ?)"
	onConflict := " ON CONFLICT DO NOTHING"

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
	}

	insertStatement := insertIntoValues + strings.Join(params, ",") + onConflict
	_, err := executor.Exec(insertStatement, bindings...)
	if err != nil {
		return err
	}

	return nil
}
