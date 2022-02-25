package rdbms

import (
	"database/sql"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
	"github.com/turnon/imdba/tsv"
)

const batch = 1000

func Import() (*sql.DB, error) {
	var db *sql.DB
	var err error
	if os.Getenv("SQLITE") != "" {
		db, err = connSqlite()
	}

	if err != nil {
		return nil, err
	}

	adb := newAsyncDb(db, 1)
	if err := batchInsertTitleBasics(adb); err != nil {
		return nil, err
	}
	adb.wait()

	return db, err
}

func connSqlite() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", os.Getenv("SQLITE"))
	if err != nil {
		return nil, err
	}

	if _, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS title_basics(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title_type TEXT NOT NULL,
        primary_title TEXT NOT NULL,
		original_title TEXT NOT NULL,
		is_adult INTEGER NOT NULL,
		start_year TEXT NOT NULL,
		end_year TEXT NOT NULL,
		runtime_minutes TEXT NOT NULL
    );
    `); err != nil {
		return nil, err
	}

	if _, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS genres(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        genre TEXT NOT NULL
    );
    `); err != nil {
		return nil, err
	}

	if _, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS title_genres(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
		title_id INTEGER NOT NULL,
        genre_id INTEGER NOT NULL
    );
    `); err != nil {
		return nil, err
	}

	return db, err
}

func batchInsertTitleBasics(adb *asyncDb) error {
	tsvDir := os.Getenv("TSV_DIR")

	genresT := newGenresTable()
	titleBasicsT := newTitleBasicsTable()

	titleBasics := make([]*tsv.TitleBasicRow, 0, batch)
	err := tsv.IterateTitleBasic(filepath.Join(tsvDir, "title.basics.tsv"), func(tb *tsv.TitleBasicRow) error {
		titleBasics = append(titleBasics, tb)
		if len(titleBasics) >= batch {
			if err := titleBasicsT.insert(adb, titleBasics...); err != nil {
				return err
			}
			if err := genresT.mapTitleGenres(adb, titleBasics...); err != nil {
				return err
			}
			titleBasics = titleBasics[0:0]
		}
		return nil
	})
	if err != nil {
		return err
	}

	if len(titleBasics) > 0 {
		if err := titleBasicsT.insert(adb, titleBasics...); err != nil {
			return err
		}
	}

	adb.done()

	return nil
}
