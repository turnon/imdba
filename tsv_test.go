package imsql

import (
	"strconv"
	"testing"
)

func TestIterateTitleBasic(t *testing.T) {
	startYears := set{}
	endYears := set{}
	genres := set{}
	titleTypes := set{}
	ids := set{}
	var maxId uint
	var count uint

	IterateTitleBasic("title.basics.tsv", func(r *TitleBasicRow, err error) error {
		if err != nil {
			t.Error(err)
			return nil
		}

		count += 1
		ids.add(strconv.FormatUint(uint64(r.Id()), 10))
		if maxId < r.Id() {
			maxId = r.Id()
		}
		startYears.add(r.StartYear)
		endYears.add(r.EndYear)
		titleTypes.add(r.TitleType)
		for _, g := range r.GenresArray() {
			genres.add(g)
		}

		return nil
	})

	t.Log("start year", startYears.sortedList())
	t.Log("end year", endYears.sortedList())
	t.Log("title type", titleTypes.sortedList())
	t.Log("genres", genres.sortedList())
	t.Log("id count", len(ids))
	t.Log("max id", maxId)
	t.Log("count", count)
}
