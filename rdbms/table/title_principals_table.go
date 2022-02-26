package table

import (
	"strings"

	"github.com/turnon/imdba/rdbms/asyncdb"
	"github.com/turnon/imdba/tsv"
)

type titlePrincipalsTable struct {
	insertStatements map[int]*string
	lastCategoryId   int
	categoryIds      map[string]int
}

func NewTitlePrincipalsTable() *titlePrincipalsTable {
	insertStatements := make(map[int]*string)
	categoryIds := make(map[string]int)
	return &titlePrincipalsTable{insertStatements: insertStatements, categoryIds: categoryIds}
}

func (tps *titlePrincipalsTable) getInsertStatement(paramsCount int) *string {
	insertStatement, ok := tps.insertStatements[paramsCount]
	if ok {
		return insertStatement
	}

	insertIntoValues := "INSERT INTO title_principals (title_id, name_id, category_id, job, characters) VALUES "
	valuesStatement := "(?, ?, ?, ?, ?)"
	valuesStatements := make([]string, 0, paramsCount)
	onConflict := " ON CONFLICT DO NOTHING"

	for ; paramsCount > 0; paramsCount -= 1 {
		valuesStatements = append(valuesStatements, valuesStatement)
	}

	originalInsertStatement := insertIntoValues + strings.Join(valuesStatements, ",") + onConflict
	tps.insertStatements[paramsCount] = &originalInsertStatement
	return &originalInsertStatement
}

func (tps *titlePrincipalsTable) getCategoryId(category string) int {
	id, ok := tps.categoryIds[category]
	if !ok {
		tps.lastCategoryId += 1
		id = tps.lastCategoryId
		tps.categoryIds[category] = id
	}
	return id
}

func (tps *titlePrincipalsTable) Insert(adb *asyncdb.AsyncDb, records ...*tsv.TitlePrincipalRow) error {
	recordCount := len(records)
	bindings := make([]interface{}, 0, recordCount*5)

	for _, r := range records {
		bindings = append(bindings, r.TitleId(), r.NameId(), tps.getCategoryId(r.Category), r.Job, r.Characters)
	}

	insertStatement := tps.getInsertStatement(recordCount)
	return adb.Exec(insertStatement, bindings)
}

func (tps *titlePrincipalsTable) InsertCategories(adb *asyncdb.AsyncDb) error {
	insertIntoValues := "INSERT INTO categories (id, category) VALUES "
	valuesStatement := "(?, ?)"
	valuesStatements := []string{}
	bindings := make([]interface{}, 0, len(tps.categoryIds)*2)

	for category, id := range tps.categoryIds {
		bindings = append(bindings, id, category)
		valuesStatements = append(valuesStatements, valuesStatement)
	}

	insertStatement := insertIntoValues + strings.Join(valuesStatements, ",")
	return adb.Exec(&insertStatement, bindings)
}
