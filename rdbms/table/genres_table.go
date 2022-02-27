package table

import (
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
	rows := 0
	mapping := []interface{}{}
	for _, r := range records {
		for _, genre := range r.GenresArray() {
			gid, ok := gh.genreIds[genre]
			if !ok {
				gh.lastGenreId += 1
				gh.genreIds[genre] = gh.lastGenreId
				gid = gh.lastGenreId
			}
			mapping = append(mapping, r.Id(), gid)
			rows += 1
		}
	}

	insertStatement := generateInsertStmt("title_genres", []string{"title_id", "genre_id"}, rows)
	return adb.Exec(&insertStatement, mapping)
}

func (gh *genresTable) Insert(adb *asyncdb.AsyncDb) error {
	insertStatement := generateInsertStmt("genres", []string{"id", "genre"}, len(gh.genreIds))

	bindings := make([]interface{}, 0, len(gh.genreIds)*2)
	for genre, id := range gh.genreIds {
		bindings = append(bindings, id, genre)
	}

	return adb.Exec(&insertStatement, bindings)
}
