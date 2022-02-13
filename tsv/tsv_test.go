package tsv

import (
	"strconv"
	"testing"

	"github.com/turnon/imdba/util"
)

func TestIterateTitleBasic(t *testing.T) {
	startYears := util.Set{}
	endYears := util.Set{}
	genres := util.Set{}
	titleTypes := util.Set{}
	ids := util.Set{}
	var maxId uint
	var count uint

	IterateTitleBasic("testdata/title.basics.tsv", func(r *TitleBasicRow) error {
		count += 1
		ids.Add(strconv.FormatUint(uint64(r.Id()), 10))
		if maxId < r.Id() {
			maxId = r.Id()
		}
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
	t.Log("id count", len(ids))
	t.Log("max id", maxId)
	t.Log("count", count)
}

func TestIterateNameBasic(t *testing.T) {
	birthYears := util.Set{}
	deathYears := util.Set{}
	professions := util.Set{}
	titles := util.Set{}
	ids := util.Set{}
	var maxId uint
	var count uint

	IterateNameBasic("testdata/name.basics.tsv", func(r *NameBasicRow) error {
		count += 1
		ids.Add(strconv.FormatUint(uint64(r.Id()), 10))
		if maxId < r.Id() {
			maxId = r.Id()
		}
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
	t.Log("id count", len(ids))
	t.Log("max id", maxId)
	t.Log("count", count)
}

func TestIterateTitlePrincipal(t *testing.T) {
	categories := util.Set{}
	characters := util.Set{}
	jobs := util.Set{}
	var count uint

	IterateTitlePrincipal("testdata/title.principals.tsv", func(r *TitlePrincipalRow) error {
		count += 1
		categories.Add(r.Category)
		jobs.Add(r.Job)
		for _, c := range r.CharactersArray() {
			characters.Add(c)
		}

		return nil
	})

	t.Log("categories", categories.SortedList())
	// t.Log("jobs", jobs.SortedList())
	// t.Log("characters", characters.SortedList())
	t.Log("count", count)
}
