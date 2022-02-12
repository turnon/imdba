package imsql

import (
	"bufio"
	"os"
	"strings"
)

type Row interface {
	Id() uint
}

type TitleBasicRow struct {
	LineNo                                                                                              uint
	Tconst, TitleType, PrimaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres string
}

func (row TitleBasicRow) Id() uint {
	return 0
}

const tab = "\t"

func LoopTsv(tsvPath string, yield func(uint, []string, error) error) error {
	f, err := os.Open(tsvPath)
	if err != nil {
		return err
	}
	defer f.Close()

	fileScanner := bufio.NewScanner(f)
	fileScanner.Split(bufio.ScanLines)

	var lineno uint
	for fileScanner.Scan() {
		lineno = lineno + 1
		if lineno == 1 {
			continue
		}
		line := fileScanner.Text()
		rec := strings.Split(line, tab)

		if yield(lineno, rec, nil) != nil {
			break
		}
	}

	return nil
}

func IterateTitleBasic(tsvPath string, yield func(*TitleBasicRow, error) error) {
	LoopTsv(tsvPath, func(lineno uint, rec []string, err error) (retErr error) {
		if err != nil {
			if err = yield(nil, err); err != nil {
				return err
			}
		}

		// if len(rec) != 9 {
		// 	msg := "wrong line:" + strconv.FormatUint(uint64(lineno), 10) + strings.Join(rec, "\t")
		// 	if err = yield(nil, errors.New(msg)); err != nil {
		// 		return err
		// 	}
		// }

		tbr := &TitleBasicRow{lineno, rec[0], rec[1], rec[2], rec[3], rec[4], rec[5], rec[6], rec[7], rec[8]}
		if err = yield(tbr, err); err != nil {
			return err
		}
		return nil
	})
}
