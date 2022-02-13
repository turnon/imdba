package tsv

import (
	"strconv"
	"strings"
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
	expectedGenres := util.NewSet(strings.Split("Action Adult Adventure Animation Biography Comedy Crime Documentary Drama Family Fantasy Film-Noir"+" "+
		"Game-Show History Horror Music Musical Mystery News Reality-TV Romance Sci-Fi Short Sport Talk-Show Thriller War Western \\N", " ")...)

	titleTypes := util.Set{}
	expectedTitleTypes := util.NewSet(strings.Split("movie short tvEpisode tvMiniSeries tvMovie tvPilot tvSeries tvShort tvSpecial video videoGame", " ")...)

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

	if !genres.IsSame(expectedGenres) {
		t.Errorf("wrong genres, expected %d, actual: %d", len(expectedGenres), len(genres))
	}

	if !titleTypes.IsSame(expectedTitleTypes) {
		t.Errorf("wrong genres, expected %d, actual: %d", len(expectedTitleTypes), len(titleTypes))
	}

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
	expectedProfessions := util.NewSet(strings.Split("actor actress animation_department art_department art_director assistant assistant_director camera_department"+" "+
		"casting_department casting_director choreographer cinematographer composer costume_department costume_designer director editor editorial_department"+" "+
		"electrical_department executive legal location_management make_up_department manager miscellaneous music_department producer production_department"+" "+
		"production_designer production_manager publicist script_department set_decorator sound_department soundtrack special_effects stunts talent_agent"+" "+
		"transportation_department visual_effects writer", " ")...)

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

		return nil
	})

	t.Log("birth year", birthYears.SortedList())
	t.Log("death year", deathYears.SortedList())

	professions.Remove("")
	if !professions.IsSame(expectedProfessions) {
		t.Errorf("wrong professions, expected %d, actual: %d", len(expectedProfessions), len(professions))
	}

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
	expectedCategories := util.NewSet(strings.Split("actor actress archive_footage archive_sound cinematographer composer director editor producer production_designer self writer", " ")...)
	var count uint

	IterateTitlePrincipal(tsv, func(r *TitlePrincipalRow) error {
		count += 1
		categories.Add(r.Category)
		return nil
	})

	if !categories.IsSame(expectedCategories) {
		t.Errorf("wrong categories, expected %d, actual: %d", len(expectedCategories), len(categories))
	}

	if count != tsLinesCount {
		t.Errorf("wrong line count, expected %d, actual: %d", tsLinesCount, count)
	}
}
