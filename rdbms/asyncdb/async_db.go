package asyncdb

import (
	"database/sql"
	"sync"

	"github.com/pkg/errors"
)

type AsyncDb struct {
	db     *sql.DB
	wg     *sync.WaitGroup
	finish chan struct{}
	writer chan<- stmtParams
	errCh  chan error
	err    error
}

type stmtParams struct {
	stmt   *string
	params []interface{}
}

func New(db *sql.DB, fns ...func(*AsyncDb) error) *AsyncDb {
	var wg sync.WaitGroup
	wg.Add(len(fns))
	finish := make(chan struct{})

	writer := make(chan stmtParams)
	errCh := make(chan error)

	adb := &AsyncDb{db: db, wg: &wg, finish: finish, writer: writer, errCh: errCh}

	go func() {
		defer close(finish)

		for stpa := range writer {
			stmt := *stpa.stmt
			if _, err := db.Exec(stmt, stpa.params...); err != nil {
				adb.err = errors.Wrap(err, stmt[0:50]+"...")
				close(errCh)
				return
			}
		}
	}()

	for _, fn := range fns {
		go func(f func(*AsyncDb) error) {
			defer wg.Done()
			f(adb)
		}(fn)
	}

	return adb
}

func (adb *AsyncDb) Exec(stmt *string, params []interface{}) error {
	select {
	case adb.writer <- stmtParams{stmt: stmt, params: params}:
	case <-adb.errCh:
	}
	return adb.err
}

func (adb *AsyncDb) Wait() {
	adb.wg.Wait()
	close(adb.writer)
	<-adb.finish
}

func (adb *AsyncDb) Error() error {
	return adb.err
}
