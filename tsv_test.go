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

	IterateTitleBasic("title.basics.tsv", func(r *TitleBasicRow) error {
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

func TestIterateNameBasic(t *testing.T) {
	birthYears := set{}
	deathYears := set{}
	professions := set{}
	titles := set{}
	ids := set{}
	var maxId uint
	var count uint

	IterateNameBasic("name.basics.tsv", func(r *NameBasicRow) error {
		count += 1
		ids.add(strconv.FormatUint(uint64(r.Id()), 10))
		if maxId < r.Id() {
			maxId = r.Id()
		}
		birthYears.add(r.BirthYear)
		deathYears.add(r.DeathYear)
		for _, p := range r.PrimaryProfessionArray() {
			professions.add(p)
		}
		for _, t := range r.KnownForTitlesArray() {
			titles.add(t)
		}

		return nil
	})

	t.Log("birth year", birthYears.sortedList())
	t.Log("death year", deathYears.sortedList())
	t.Log("titles", len(titles))
	t.Log("professions", professions.sortedList())
	t.Log("id count", len(ids))
	t.Log("max id", maxId)
	t.Log("count", count)
}

func TestIterateTitlePrincipal(t *testing.T) {
	categories := set{}
	characters := set{}
	jobs := set{}
	var count uint

	IterateTitlePrincipal("title.principals.tsv", func(r *TitlePrincipalRow) error {
		count += 1
		categories.add(r.Category)
		jobs.add(r.Job)
		for _, c := range r.CharactersArray() {
			characters.add(c)
		}

		return nil
	})

	t.Log("categories", categories.sortedList())
	// t.Log("jobs", jobs.sortedList())
	// t.Log("characters", characters.sortedList())
	t.Log("count", count)
}
