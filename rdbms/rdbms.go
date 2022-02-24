package rdbms

import (
	"strings"

	"github.com/turnon/imdba/tsv"
)

type genresHandler struct {
	lastGenreId int
	genreIds    map[string]int
}

func newGenresHandler() *genresHandler {
	genreIds := make(map[string]int)
	return &genresHandler{genreIds: genreIds}
}

func (gh *genresHandler) mapTitleGenres(db *asyncDb, records ...*tsv.TitleBasicRow) error {
	insertIntoValues := "INSERT INTO title_genres (title_id, genre_id) VALUES "
	valuesStatement := "(?, ?)"
	valuesStatements := []string{}
	mapping := []interface{}{}

	for _, r := range records {
		for _, genre := range r.GenresArray() {
			gid, ok := gh.genreIds[genre]
			if !ok {
				gh.lastGenreId += 1
				gh.genreIds[genre] = gh.lastGenreId
			}
			mapping = append(mapping, r.Id(), gid)
			valuesStatements = append(valuesStatements, valuesStatement)
		}
	}

	insertStatement := insertIntoValues + strings.Join(valuesStatements, ",")
	db.exec(&insertStatement, mapping)

	return nil
}

func InsertTitleBasics(executor *asyncDb, records ...*tsv.TitleBasicRow) error {
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
	executor.exec(&insertStatement, bindings)

	return nil
}
