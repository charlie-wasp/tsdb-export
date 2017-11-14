package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
  "errors"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

type series struct {
	Labels labels.Labels
	Points []point
}

type point struct {
	T int64
	V float64
}

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

func parseLabelsString(s string) map[string]string {
	s = strings.Trim(s, "{}")
	result := make(map[string]string)

	for _, stringPair := range strings.Split(s, ",") {
		pair := strings.Split(stringPair, "=")
		result[pair[0]] = strings.Trim(pair[1], "\"")
	}

	return result
}

func connectToTsdb(path string) (db *tsdb.DB, err error) {
  stat, err := os.Stat(path)
  if err != nil { return nil, err }
  if !stat.IsDir() {
    return nil, errors.New(fmt.Sprintf("`%s` is not a directory", path))
  }

	r := prometheus.NewRegistry()
	logger := log.NewLogfmtLogger(os.Stdout)

	options := tsdb.DefaultOptions
	options.NoLockfile = true

	return tsdb.Open(path, logger, r, options)
}

func writeLabelsFile(idsLabelsMap map[string]int64) {
	labelsFile, err := os.Create("labels.csv")

	if err != nil {
		panic(err)
	}

	labelsIdsCsvWriter := csv.NewWriter(labelsFile)

	defer labelsFile.Close()
	defer labelsIdsCsvWriter.Flush()

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

func main() {
	var labelsFilter labelMatchers
	var seriesIdsCounter int64 = 1
	idsLabelsMap := make(map[string]int64)

	mintString := flag.String("start", "", "Min time")
	maxtString := flag.String("end", "", "Max time")
	path := flag.String("db-path", "", "Path to tsdb directory")
	flag.Var(&labelsFilter, "label-filter", "")

	flag.Parse()

	db, err := connectToTsdb(*path)

	if err != nil {
		fmt.Println(err)
    os.Exit(1)
	}

	mint, err := time.Parse(time.RFC3339, *mintString)
	maxt, err := time.Parse(time.RFC3339, *maxtString)

	querier, err := db.Querier(mint.Unix()*1000, maxt.Unix()*1000)

	if err != nil {
		panic(err)
	}

	defer querier.Close()

	ss := querier.Select(labelsFilter...)

	pointsFile, err := os.Create("points.csv")

	if err != nil {
		panic(err)
	}

	pointsCsvWriter := csv.NewWriter(pointsFile)

	defer pointsFile.Close()
	defer pointsCsvWriter.Flush()

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

		for it.Next() {
			t, v := it.At()

			pointsCsvWriter.Write([]string{
				fmt.Sprintf("%d", idsLabelsMap[labelsString]),
				fmt.Sprintf("%d", t),
				fmt.Sprintf("%v", v),
			})
		}
	}

	writeLabelsFile(idsLabelsMap)
}
