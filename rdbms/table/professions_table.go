package table

import (
	"strings"

	"github.com/turnon/imdba/rdbms/asyncdb"
	"github.com/turnon/imdba/tsv"
)

type professionsTable struct {
	lastProfessionId int
	professionIds    map[string]int
}

func NewProfessionsTable() *professionsTable {
	professionIds := make(map[string]int)
	return &professionsTable{professionIds: professionIds}
}

func (pt *professionsTable) MapNameProfessions(adb *asyncdb.AsyncDb, records ...*tsv.NameBasicRow) error {
	insertIntoValues := "INSERT INTO name_professions (name_id, profession_id) VALUES "
	valuesStatement := "(?, ?)"
	valuesStatements := make([]string, 0, len(records)*2)
	mapping := []interface{}{}

	for _, r := range records {
		for _, profession := range r.PrimaryProfessionArray() {
			pid, ok := pt.professionIds[profession]
			if !ok {
				pt.lastProfessionId += 1
				pt.professionIds[profession] = pt.lastProfessionId
				pid = pt.lastProfessionId
			}
			mapping = append(mapping, r.Id(), pid)
			valuesStatements = append(valuesStatements, valuesStatement)
		}
	}

	insertStatement := insertIntoValues + strings.Join(valuesStatements, ",")
	return adb.Exec(&insertStatement, mapping)
}

func (pt *professionsTable) Insert(adb *asyncdb.AsyncDb) error {
	insertIntoValues := "INSERT INTO professions (id, profession) VALUES "
	valuesStatement := "(?, ?)"
	valuesStatements := []string{}
	bindings := make([]interface{}, 0, len(pt.professionIds)*2)

	for profession, id := range pt.professionIds {
		bindings = append(bindings, id, profession)
		valuesStatements = append(valuesStatements, valuesStatement)
	}

	insertStatement := insertIntoValues + strings.Join(valuesStatements, ",")
	return adb.Exec(&insertStatement, bindings)
}
