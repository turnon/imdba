package rdbms

import (
	"strings"

	"github.com/turnon/imdba/tsv"
)

type genresTable struct {
	lastGenreId int
	genreIds    map[string]int
}

func newGenresTable() *genresTable {
	genreIds := make(map[string]int)
	return &genresTable{genreIds: genreIds}
}

func (gh *genresTable) mapTitleGenres(db *asyncDb, records ...*tsv.TitleBasicRow) error {
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
