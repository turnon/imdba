package table

import (
	"strings"

	"github.com/turnon/imdba/rdbms/asyncdb"
	"github.com/turnon/imdba/tsv"
)

func MapNameTitles(adb *asyncdb.AsyncDb, records ...*tsv.NameBasicRow) error {
	insertIntoValues := "INSERT INTO name_titles (name_id, title_id) VALUES "
	valuesStatement := "(?, ?)"
	valuesStatements := make([]string, 0, len(records)*4)
	mapping := []interface{}{}

	for _, r := range records {
		for _, tid := range r.KnownForTitleIdsArray() {
			valuesStatements = append(valuesStatements, valuesStatement)
			mapping = append(mapping, r.Id(), tid)
		}
	}

	insertStatement := insertIntoValues + strings.Join(valuesStatements, ",")
	return adb.Exec(&insertStatement, mapping)
}
