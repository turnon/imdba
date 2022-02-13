package tsv

import (
	"strconv"
	"testing"

	"github.com/turnon/imdba/util"
)

func TestIterateTitleBasic(t *testing.T) {
	const tsv = "testdata/title.basics.tsv"
	tsLinesCount, err := CountLine(tsv)
	if err != nil {
		t.Errorf("fail to count lines in %s", tsv)
	}

	startYears := util.Set{}
	endYears := util.Set{}
	genres := util.Set{}
	titleTypes := util.Set{}
	ids := util.Set{}
	var count uint

	IterateTitleBasic(tsv, func(r *TitleBasicRow) error {
		count += 1
		ids.Add(strconv.FormatUint(uint64(r.Id()), 10))
		startYears.Add(r.StartYear)
		endYears.Add(r.EndYear)
		titleTypes.Add(r.TitleType)
		for _, g := range r.GenresArray() {
			genres.Add(g)
		}

		return nil
	})

	t.Log("start year", startYears.SortedList())
	t.Log("end year", endYears.SortedList())
	t.Log("title type", titleTypes.SortedList())
	t.Log("genres", genres.SortedList())

	if idCount := uint(len(ids)); idCount != tsLinesCount {
		t.Errorf("wrong id count, expected %d, actual: %d", tsLinesCount, idCount)
	}
	if count != tsLinesCount {
		t.Errorf("wrong line count, expected %d, actual: %d", tsLinesCount, count)
	}
}

func TestIterateNameBasic(t *testing.T) {
	const tsv = "testdata/name.basics.tsv"
	tsLinesCount, err := CountLine(tsv)
	if err != nil {
		t.Errorf("fail to count lines in %s", tsv)
	}

	birthYears := util.Set{}
	deathYears := util.Set{}
	professions := util.Set{}
	titles := util.Set{}
	ids := util.Set{}
	var count uint

	IterateNameBasic(tsv, func(r *NameBasicRow) error {
		count += 1
		ids.Add(strconv.FormatUint(uint64(r.Id()), 10))
		birthYears.Add(r.BirthYear)
		deathYears.Add(r.DeathYear)
		for _, p := range r.PrimaryProfessionArray() {
			professions.Add(p)
		}
		for _, t := range r.KnownForTitlesArray() {
			titles.Add(t)
		}

		return nil
	})

	t.Log("birth year", birthYears.SortedList())
	t.Log("death year", deathYears.SortedList())
	t.Log("titles", len(titles))
	t.Log("professions", professions.SortedList())

	if idCount := uint(len(ids)); idCount != tsLinesCount {
		t.Errorf("wrong id count, expected %d, actual: %d", tsLinesCount, idCount)
	}
	if count != tsLinesCount {
		t.Errorf("wrong line count, expected %d, actual: %d", tsLinesCount, count)
	}
}

func TestIterateTitlePrincipal(t *testing.T) {
	const tsv = "testdata/title.principals.tsv"
	tsLinesCount, err := CountLine(tsv)
	if err != nil {
		t.Errorf("fail to count lines in %s", tsv)
	}

	categories := util.Set{}
	characters := util.Set{}
	jobs := util.Set{}
	var count uint

	IterateTitlePrincipal(tsv, func(r *TitlePrincipalRow) error {
		count += 1
		categories.Add(r.Category)
		jobs.Add(r.Job)
		for _, c := range r.CharactersArray() {
			characters.Add(c)
		}

		return nil
	})

	t.Log("categories", categories.SortedList())

	if count != tsLinesCount {
		t.Errorf("wrong line count, expected %d, actual: %d", tsLinesCount, count)
	}
}
