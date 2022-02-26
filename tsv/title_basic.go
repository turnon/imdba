package tsv

import (
	"strings"
)

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
	r.id = tt2Int(r.Tconst)
	return r.id
}

func IterateTitleBasic(tsvPath string, yield func(*TitleBasicRow) error) error {
	return LoopTsv(tsvPath, func(lineno uint, rec []string) error {
		tbr := &TitleBasicRow{0, lineno, rec[0], rec[1], rec[2], rec[3], rec[4], rec[5], rec[6], rec[7], rec[8]}
		return yield(tbr)
	})
}
