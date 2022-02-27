package table

import (
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
	rows := 0
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
			rows += 1
		}
	}

	insertStatement := generateInsertStmt("name_professions", []string{"name_id", "profession_id"}, rows)
	return adb.Exec(&insertStatement, mapping)
}

func (pt *professionsTable) Insert(adb *asyncdb.AsyncDb) error {
	bindings := make([]interface{}, 0, len(pt.professionIds)*2)
	for profession, id := range pt.professionIds {
		bindings = append(bindings, id, profession)
	}

	insertStatement := generateInsertStmt("professions", []string{"id", "profession"}, len(pt.professionIds))
	return adb.Exec(&insertStatement, bindings)
}
