package tsv2sql

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	sql_interface "github.com/turnon/imdba/sql"
	"github.com/turnon/imdba/tsv"
)

func TestInsertTitleBasics(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectExec("INSERT INTO title_basics").
		WithArgs(133093, "movie", "The Matrix", "The Matrix", "0", "1999", "\\N", "136", "Action,Sci-Fi").
		WillReturnResult(sqlmock.NewResult(1, 1))

	// mock.ExpectExec("INSERT INTO title_basics").
	// 	WithArgs(234215, "movie", "The Matrix Reloaded", "The Matrix Reloaded", "0", "2003", "\\N", "138", "Action,Sci-Fi").
	// 	WillReturnResult(sqlmock.NewResult(1, 1))

	tsv.IterateTitleBasic("testdata/title.basics.abstract.tsv", func(r *tsv.TitleBasicRow) error {
		return sql_interface.InsertTitleBasics(db, r)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
