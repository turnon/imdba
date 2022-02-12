package imsql

import (
	"bufio"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var ttIdRegexp = regexp.MustCompile(`tt[0]*`)

type TitleBasicRow struct {
	id, LineNo                                                                                          uint
	Tconst, TitleType, PrimaryTitle, OriginalTitle, IsAdult, StartYear, EndYear, RuntimeMinutes, Genres string
}

func (r *TitleBasicRow) GenresArray() []string {
	return strings.Split(r.Genres, ",")
}

func (r *TitleBasicRow) Id() uint {
	if r.id != 0 {
		return r.id
	}
	idStr := ttIdRegexp.ReplaceAllString(r.Tconst, "")
	i, _ := strconv.ParseUint(idStr, 0, 64)
	r.id = uint(i)
	return r.id
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

		tbr := &TitleBasicRow{0, lineno, rec[0], rec[1], rec[2], rec[3], rec[4], rec[5], rec[6], rec[7], rec[8]}
		if err = yield(tbr, err); err != nil {
			return err
		}
		return nil
	})
}
