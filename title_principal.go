package imsql

import "strings"

type TitlePrincipalRow struct {
	LineNo                                              uint
	Tconst, Ordering, Nconst, Category, Job, Characters string
}

func (r *TitlePrincipalRow) CharactersArray() []string {
	return strings.Split(r.Characters, ",")
}

func IterateTitlePrincipal(tsvPath string, yield func(*TitlePrincipalRow) error) {
	LoopTsv(tsvPath, func(lineno uint, rec []string) error {
		tpr := &TitlePrincipalRow{lineno, rec[0], rec[1], rec[2], rec[3], rec[4], rec[5]}
		return yield(tpr)
	})
}
