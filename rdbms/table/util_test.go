package table

import "testing"

func TestGenerateInsertStmt(t *testing.T) {
	stmt := generateInsertStmt("people", []string{"id", "name", "gender"}, 3)
	if stmt != "INSERT INTO people (id,name,gender ) VALUES ($1,$2,$3),($4,$5,$6),($7,$8,$9)" {
		t.Error(stmt)
	}
}
