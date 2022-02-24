package rdbms

import (
	"database/sql"
	"sync"
)

type asyncDb struct {
	db     *sql.DB
	wg     sync.WaitGroup
	finish chan struct{}
	writer chan<- stmtParams
}

type stmtParams struct {
	stmt   *string
	params []interface{}
}

func newAsyncDb(db *sql.DB, delta int) *asyncDb {
	var wg sync.WaitGroup
	wg.Add(delta)
	finish := make(chan struct{})

	writer := make(chan stmtParams)
	go func() {
		for stpa := range writer {
			db.Exec(*stpa.stmt, stpa.params...)
		}
		close(finish)
	}()

	return &asyncDb{db: db, wg: wg, finish: finish, writer: writer}
}

func (adb *asyncDb) exec(stmt *string, params []interface{}) {
	adb.writer <- stmtParams{stmt: stmt, params: params}
}

func (adb *asyncDb) done() {
	adb.wg.Done()
}

func (adb *asyncDb) wait() {
	adb.wg.Wait()
	close(adb.writer)
	<-adb.finish
}
