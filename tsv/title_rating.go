package tsv

import (
	"strconv"
	"strings"
)

type TitleRatingRow struct {
	id, LineNo                  uint
	Tconst, AvgRating, NumVotes string
}

func (r *TitleRatingRow) TtId() uint {
	if r.id != 0 {
		return r.id
	}
	r.id = tt2Int(r.Tconst)
	return r.id
}

func (r *TitleRatingRow) AvgRatingInt() int64 {
	rate := strings.ReplaceAll(r.AvgRating, ".", "")
	i, _ := strconv.ParseInt(rate, 0, 8)
	return i
}

func (r *TitleRatingRow) NumVotesInt() int {
	votes := strings.ReplaceAll(r.NumVotes, ".", "")
	i, _ := strconv.ParseInt(votes, 0, 32)
	return int(i)
}

func IterateTitleRating(tsvPath string, yield func(*TitleRatingRow) error) error {
	return LoopTsv(tsvPath, func(lineno uint, rec []string) error {
		trr := &TitleRatingRow{0, lineno, rec[0], rec[1], rec[2]}
		return yield(trr)
	})
}
