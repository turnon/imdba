package table

import (
	"github.com/turnon/imdba/rdbms/asyncdb"
	"github.com/turnon/imdba/tsv"
)

func MapNameTitles(adb *asyncdb.AsyncDb, records ...*tsv.NameBasicRow) error {
	rows := 0
	mapping := []interface{}{}
	for _, r := range records {
		for _, tid := range r.KnownForTitleIdsArray() {
			mapping = append(mapping, r.Id(), tid)
			rows += 1
		}
	}

	insertStatement := generateInsertStmt("name_titles", []string{"name_id", "title_id"}, rows)
	return adb.Exec(&insertStatement, mapping)
}
