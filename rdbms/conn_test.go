package rdbms

import (
	"os"
	"testing"
)

func TestImportSqlite(t *testing.T) {
	os.Setenv("SQLITE", "test_sqlite.db")
	os.Setenv("TSV_DIR", "testdata")

	db, err := Import()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	result, err := db.Query("SELECT count(*) FROM title_basics")
	if err != nil {
		t.Error(err)
	}

	var count int
	defer result.Close()
	for result.Next() {
		err := result.Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%d\n", count)
	}
}

func TestImportPg(t *testing.T) {
	os.Setenv("PG", "host=192.168.0.109 port=5432 user=root password=mysecretpassword dbname=pig sslmode=disable")
	os.Setenv("TSV_DIR", "testdata")

	db, err := Import()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	result, err := db.Query("SELECT count(*) FROM genres")
	if err != nil {
		t.Error(err)
	}

	var count int
	defer result.Close()
	for result.Next() {
		err := result.Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%d\n", count)
	}
}
