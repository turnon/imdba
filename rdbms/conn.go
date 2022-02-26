package rdbms

import (
	"database/sql"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
	"github.com/turnon/imdba/rdbms/asyncdb"
	"github.com/turnon/imdba/rdbms/table"
	"github.com/turnon/imdba/tsv"
)

const batch = 4000

func Import() (*sql.DB, error) {
	var db *sql.DB
	var err error
	if os.Getenv("SQLITE") != "" {
		db, err = connSqlite()
	}

	if err != nil {
		return nil, err
	}

	adb := asyncdb.New(db, batchInsertTitleBasics, batchInsertNameBasics)

	adb.Wait()

	if err = adb.Error(); err != nil {
		return nil, err
	}

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

	if _, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS name_basics(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
		primary_name TEXT NOT NULL,
        birth_year TEXT NOT NULL,
		death_year TEXT NOT NULL
    );
    `); err != nil {
		return nil, err
	}

	if _, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS name_professions(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
		name_id INTEGER NOT NULL,
        profession_id INTEGER NOT NULL
    );
    `); err != nil {
		return nil, err
	}

	if _, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS professions(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        profession TEXT NOT NULL
    );
    `); err != nil {
		return nil, err
	}

	return db, err
}

func batchInsertTitleBasics(adb *asyncdb.AsyncDb) error {

	tsvDir := os.Getenv("TSV_DIR")

	genresT := table.NewGenresTable()
	titleBasicsT := table.NewTitleBasicsTable()

	titleBasics := make([]*tsv.TitleBasicRow, 0, batch)
	err := tsv.IterateTitleBasic(filepath.Join(tsvDir, "title.basics.tsv"), func(tb *tsv.TitleBasicRow) error {
		titleBasics = append(titleBasics, tb)
		if len(titleBasics) >= batch {
			if err := titleBasicsT.Insert(adb, titleBasics...); err != nil {
				return err
			}
			if err := genresT.MapTitleGenres(adb, titleBasics...); err != nil {
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
		if err := titleBasicsT.Insert(adb, titleBasics...); err != nil {
			return err
		}
	}

	return genresT.Insert(adb)
}

func batchInsertNameBasics(adb *asyncdb.AsyncDb) error {
	tsvDir := os.Getenv("TSV_DIR")

	professionsT := table.NewProfessionsTable()
	nameBasicsT := table.NewNameBasicsTable()

	nameBasics := make([]*tsv.NameBasicRow, 0, batch)
	err := tsv.IterateNameBasic(filepath.Join(tsvDir, "name.basics.tsv"), func(nb *tsv.NameBasicRow) error {
		nameBasics = append(nameBasics, nb)
		if len(nameBasics) >= batch {
			if err := nameBasicsT.Insert(adb, nameBasics...); err != nil {
				return err
			}
			if err := professionsT.MapNameProfessions(adb, nameBasics...); err != nil {
				return err
			}
			nameBasics = nameBasics[0:0]
		}
		return nil
	})
	if err != nil {
		return err
	}

	if len(nameBasics) > 0 {
		if err := nameBasicsT.Insert(adb, nameBasics...); err != nil {
			return err
		}
	}

	return professionsT.Insert(adb)
}
