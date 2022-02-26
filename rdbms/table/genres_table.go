package table

import (
	"strings"

	"github.com/turnon/imdba/rdbms/asyncdb"
	"github.com/turnon/imdba/tsv"
)

type genresTable struct {
	lastGenreId int
	genreIds    map[string]int
}

func NewGenresTable() *genresTable {
	genreIds := make(map[string]int)
	return &genresTable{genreIds: genreIds}
}

func (gh *genresTable) MapTitleGenres(adb *asyncdb.AsyncDb, records ...*tsv.TitleBasicRow) error {
	insertIntoValues := "INSERT INTO title_genres (title_id, genre_id) VALUES "
	valuesStatement := "(?, ?)"
	valuesStatements := make([]string, 0, len(records)*2)
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
	return adb.Exec(&insertStatement, mapping)
}

func (gh *genresTable) Insert(adb *asyncdb.AsyncDb) error {
	insertIntoValues := "INSERT INTO genres (id, genre) VALUES "
	valuesStatement := "(?, ?)"
	valuesStatements := []string{}
	bindings := make([]interface{}, 0, len(gh.genreIds)*2)

	for genre, id := range gh.genreIds {
		bindings = append(bindings, id, genre)
		valuesStatements = append(valuesStatements, valuesStatement)
	}

	insertStatement := insertIntoValues + strings.Join(valuesStatements, ",")
	return adb.Exec(&insertStatement, bindings)
}
