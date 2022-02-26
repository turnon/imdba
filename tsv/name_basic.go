package tsv

import (
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
	r.id = nm2Int(r.Nconst)
	return r.id
}

func (r *NameBasicRow) PrimaryProfessionArray() []string {
	return strings.Split(r.PrimaryProfession, ",")
}

func (r *NameBasicRow) KnownForTitlesArray() []string {
	return strings.Split(r.KnownForTitles, ",")
}

func (r *NameBasicRow) KnownForTitleIdsArray() []uint {
	tArr := r.KnownForTitlesArray()
	ids := make([]uint, 0, len(tArr))
	for _, tt := range tArr {
		ids = append(ids, tt2Int(tt))
	}
	return ids
}

func IterateNameBasic(tsvPath string, yield func(*NameBasicRow) error) error {
	return LoopTsv(tsvPath, func(lineno uint, rec []string) error {
		nbr := &NameBasicRow{0, lineno, rec[0], rec[1], rec[2], rec[3], rec[4], rec[5]}
		return yield(nbr)
	})
}
