package rdbms

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/turnon/imdba/tsv"
)

func TestInsertTitleBasics(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectExec("INSERT INTO title_basics").
		WithArgs(133093, "movie", "The Matrix", "The Matrix", "0", "1999", "\\N", "136",
			234215, "movie", "The Matrix Reloaded", "The Matrix Reloaded", "0", "2003", "\\N", "138").
		WillReturnResult(sqlmock.NewResult(1, 1))

	records := []*tsv.TitleBasicRow{
		{Tconst: "tt0133093", TitleType: "movie", PrimaryTitle: "The Matrix", OriginalTitle: "The Matrix", IsAdult: "0", StartYear: "1999", EndYear: "\\N", RuntimeMinutes: "136", Genres: "Action,Sci-Fi"},
		{Tconst: "tt0234215", TitleType: "movie", PrimaryTitle: "The Matrix Reloaded", OriginalTitle: "The Matrix Reloaded", IsAdult: "0", StartYear: "2003", EndYear: "\\N", RuntimeMinutes: "138", Genres: "Action,Sci-Fi"},
	}
	InsertTitleBasics(db, records...)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
