package imsql

import (
	"strconv"
	"strings"
)

type NameBasicRow struct {
	id, LineNo                                                                   uint
	Nconst, PrimaryName, BirthYear, DeathYear, PrimaryProfession, KnownForTitles string
}

func (r *NameBasicRow) Id() uint {
	if r.id != 0 {
		return r.id
	}
	idStr := nIdRegexp.ReplaceAllString(r.Nconst, "")
	i, _ := strconv.ParseUint(idStr, 0, 32)
	r.id = uint(i)
	return r.id
}

func (r *NameBasicRow) PrimaryProfessionArray() []string {
	return strings.Split(r.PrimaryProfession, ",")
}

func (r *NameBasicRow) KnownForTitlesArray() []string {
	return strings.Split(r.KnownForTitles, ",")
}

func IterateNameBasic(tsvPath string, yield func(*NameBasicRow, error) error) {
	LoopTsv(tsvPath, func(lineno uint, rec []string, err error) (retErr error) {
		if err != nil {
			if err = yield(nil, err); err != nil {
				return err
			}
		}

		nbr := &NameBasicRow{0, lineno, rec[0], rec[1], rec[2], rec[3], rec[4], rec[5]}
		if err = yield(nbr, err); err != nil {
			return err
		}
		return nil
	})
}
