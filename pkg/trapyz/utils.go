package trapyz

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path"
	"regexp"
	"strings"
	"time"
)

//ErrorInvalidDate an error passed to upstream modules
var ErrorInvalidDate = errors.New("Invalid Date Time format")

//ErrorStartAfterEnd an error thrown when a from date falls after the to date
var ErrorStartAfterEnd = errors.New("Invalid from and to dates from-date is after to-date")

const dateFormat = "2006/01/02"
const dateHourFormat = "2006/01/02/15"

var dateRegex = regexp.MustCompile(`^(\d{4})/(\d{2})/(\d{2})?$`)

//CfgKey returns a config key depending on the tpz_env config
//variable in the config file
func CfgKey(cfg *Config, dbtype string) string {
	defSuffix := "dev"
	if cfg.TpzEnv != "" {
		return fmt.Sprintf("%s-%s", dbtype, cfg.TpzEnv)
	}
	return fmt.Sprintf("%s-%s", dbtype, defSuffix)
}

//FindAndCreateDestDir does what the function says
func FindAndCreateDestDir(cfg *Config) string {
	//Get the Dump Prefix
	awsCfgInfo := cfg.Aws[CfgKey(cfg, "s3")]
	dumpDir := awsCfgInfo.S3dumpPrefix + "-" + cfg.TpzEnv
	//Create the dump directory
	os.MkdirAll(dumpDir, 0755)
	return dumpDir
}

func expandTilde(path string) string {
	if strings.HasPrefix(path, "~") {
		usr, err := user.Current()
		if err != nil {
			return path
		}
		home := usr.HomeDir
		return home + path[1:]
	}
	return path
}

func roundToHour(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
}

func roundToDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

func spanDays(fd time.Time, td time.Time) int {
	return int(td.Sub(fd).Hours() / 24)
}

func datePrefixGenerator(fromDate time.Time, toDate time.Time) []string {
	var prefixes []string

	nd := fromDate.AddDate(0, 0, 1)
	n := spanDays(roundToDay(nd), roundToDay(toDate))

	if fromDate.Hour() == 0 && n > 0 {
		prefixes = append(prefixes, fromDate.Format(dateFormat))
	} else {
		for i := fromDate.Hour(); i < 24; i++ {
			str := fmt.Sprintf("%s/%02d", fromDate.Format(dateFormat), i)
			prefixes = append(prefixes, str)
		}
	}

	for i := 0; i < n; i++ {
		nextDay := nd.AddDate(0, 0, i)
		prefixes = append(prefixes, nextDay.Format(dateFormat))

	}

	if toDate.Hour() != 0 {
		for i := 0; i < toDate.Hour(); i++ {
			str := fmt.Sprintf("%s/%02d", toDate.Format(dateFormat), i)
			prefixes = append(prefixes, str)
		}
	}
	return prefixes
}

//ParseDate parses a date, a date/hour or throws an error
func ParseDate(dateStr string) (time.Time, error) {
	if didMatch := dateRegex.MatchString(dateStr); didMatch {
		dt, err := time.Parse(dateFormat, dateStr)
		return dt, err
	}
	return time.Parse(dateHourFormat, dateStr)
}

//ParseDates parses a start (fd) and end (td) date strings into time.Time objects
func ParseDates(fd string, td string) (time.Time, time.Time, error) {
	var start, end time.Time

	if fd == "" {
		start = roundToDay(time.Now().AddDate(0, 0, -1))
	} else {
		res, err := ParseDate(fd)
		if nil != err {
			return start, end, err
		}
		start = res
	}
	if td == "" {
		end = roundToHour(time.Now())
	} else {
		res, err := ParseDate(td)
		if nil != err {
			return start, end, err
		}
		end = res
	}
	if !start.Before(end) {
		return start, end, ErrorStartAfterEnd
	}
	return start, end, nil
}

//PrefixChan provides a channel of prefixes so that multiple go routines can consume a prefix in parallel
func PrefixChan(ctx context.Context, fromDate string, toDate string, topPrefixes []string) (<-chan string, error) {
	start, end, err := ParseDates(fromDate, toDate)
	if err != nil {
		return nil, err
	}

	datePrefixes := datePrefixGenerator(start, end)
	var n = len(topPrefixes) * len(datePrefixes)
	allPrefixes := make([]string, 0, n)
	for _, tp := range topPrefixes {
		for _, dp := range datePrefixes {
			allPrefixes = append(allPrefixes, path.Join(tp, dp))
		}
	}
	out := make(chan string)
	go func() {
		var cur = 0
		for {
			select {
			case out <- allPrefixes[cur]:
				if cur++; cur == n {
					close(out)
					return
				}
			case <-ctx.Done():
				close(out)
				return
			}
		}
	}()
	return out, nil
}