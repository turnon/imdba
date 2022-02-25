package asyncdb

import (
	"database/sql"
	"sync"
)

type AsyncDb struct {
	db     *sql.DB
	wg     sync.WaitGroup
	finish chan struct{}
	writer chan<- stmtParams
}

type stmtParams struct {
	stmt   *string
	params []interface{}
}

func NewAsyncDb(db *sql.DB, delta int) *AsyncDb {
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

	return &AsyncDb{db: db, wg: wg, finish: finish, writer: writer}
}

func (adb *AsyncDb) Exec(stmt *string, params []interface{}) {
	adb.writer <- stmtParams{stmt: stmt, params: params}
}

func (adb *AsyncDb) Done() {
	adb.wg.Done()
}

func (adb *AsyncDb) Wait() {
	adb.wg.Wait()
	close(adb.writer)
	<-adb.finish
}
