package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

type labelMatchers []labels.Matcher

func (i labelMatchers) String() string {
	return "adasd"
}

func (i *labelMatchers) Set(value string) error {
	parsedValue := strings.Split(value, "=")
  m := labels.NewEqualMatcher(parsedValue[0], parsedValue[1])
	*i = append(*i, m)

	return nil
}

type series struct {
	Labels labels.Labels
	Points []point
}

type point struct {
	T int64
	V float64
}

func parseLabelsString(s string) map[string]string {
	s = strings.Trim(s, "{}")
	result := make(map[string]string)

	for _, stringPair := range strings.Split(s, ",") {
		pair := strings.Split(stringPair, "=")
		result[pair[0]] = strings.Trim(pair[1], "\"")
	}

	return result
}

func main() {
	// p := fmt.Println

	r := prometheus.NewRegistry()
	logger := log.NewLogfmtLogger(os.Stdout)

	options := tsdb.DefaultOptions
	options.NoLockfile = true

	var labelsFilter labelMatchers

	mintString := flag.String("mint", "", "Min time")
	maxtString := flag.String("maxt", "", "Max time")
	path := flag.String("db-path", "", "Path to tsdb directory")
	flag.Var(&labelsFilter, "label-filter", "")

	flag.Parse()

	db, err := tsdb.Open(*path, logger, r, options)

	if err != nil {
		panic(err)
	}

	mint, err := time.Parse(time.RFC3339, *mintString)
	maxt, err := time.Parse(time.RFC3339, *maxtString)

	querier, err := db.Querier(mint.Unix()*1000, maxt.Unix()*1000)

	matchedSer := make([]series, 0)

	if err != nil {
		panic(err)
	}

	defer querier.Close()

	ss := querier.Select(labelsFilter...)

	var seriesIdsCounter int64 = 1
	idsLabelsMap := make(map[string]int64)
	pointsFile, err := os.Create("points.csv")

	if err != nil {
		panic(err)
	}

	labelsFile, err := os.Create("labels.csv")

	if err != nil {
		panic(err)
	}

	pointsCsvWriter := csv.NewWriter(pointsFile)
	labelsIdsCsvWriter := csv.NewWriter(labelsFile)

	defer pointsFile.Close()
	defer labelsFile.Close()
	defer pointsCsvWriter.Flush()
	defer labelsIdsCsvWriter.Flush()

	pointsCsvWriter.Write([]string{"id", "timestamp", "value"})

	for ss.Next() {
		s := ss.At()
		labels := s.Labels()
		labelsString := labels.String()

		if _, prs := idsLabelsMap[labelsString]; !prs {
			idsLabelsMap[labelsString] = seriesIdsCounter
			seriesIdsCounter += 1
		}

		it := s.Iterator()

		pts := make([]point, 0)

		for it.Next() {
			t, v := it.At()
			pts = append(pts, point{t, v})

			pointsCsvWriter.Write([]string{
				fmt.Sprintf("%d", idsLabelsMap[labelsString]),
				fmt.Sprintf("%d", t),
				fmt.Sprintf("%v", v),
			})
		}

		matchedSer = append(matchedSer, series{labels, pts})
	}

	labelsIdsCsvWriter.Write([]string{"id", "label_name", "label_value"})

	for lablesString, id := range idsLabelsMap {
		lablesMap := parseLabelsString(lablesString)

		for labelName, labelValue := range lablesMap {
			labelsIdsCsvWriter.Write([]string{
				fmt.Sprintf("%d", id),
				labelName,
				labelValue,
			})
		}
	}
}
