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

//DateFormat the date format used for parsing dates
const DateFormat = "2006/01/02"

//DateHourFormat the date hour format for parsing dates
const DateHourFormat = "2006/01/02/15"

var dateRegex = regexp.MustCompile(`^(\d{4})/(\d{2})/(\d{2})$`)

//CfgKey returns a config key depending on the tpz_env config
//variable in the config file
func CfgKey(cfg *Config, dbtype string) string {
	defSuffix := "dev"
	if cfg.TpzEnv != "" {
		return fmt.Sprintf("%s-%s", dbtype, cfg.TpzEnv)
	}
	return fmt.Sprintf("%s-%s", dbtype, defSuffix)
}

//FindOrCreateDestDir does what the function says
func FindOrCreateDestDir(cfg *Config) string {
	//Get the Dump Prefix
	awsCfgInfo := cfg.Aws[CfgKey(cfg, "s3")]
	dumpDir := awsCfgInfo.S3dumpPrefix + "-" + cfg.TpzEnv
	//Create the dump directory
	if _, err := os.Stat(dumpDir); os.IsNotExist(err) {
		os.MkdirAll(dumpDir, 0755)
	}
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

//GetStartEndTime returns the Start and End times for the next run
func GetStartEndTime(schedType string, start, end time.Time) (time.Time, time.Time) {
	now := time.Now()
	var nextStart, nextEnd time.Time
	if "hourly" == schedType {
		if end.IsZero() {
			nextStart = roundToHour(now).Add(-1 * time.Hour)
			nextEnd = roundToHour(now)
		} else {
			nextStart = roundToHour(end)
			nextEnd = nextStart.Add(1 * time.Hour)
		}
	}

	if "daily" == schedType {
		if end.IsZero() {
			nextStart = roundToHour(now).Add(-1 * time.Hour)
			nextEnd = roundToHour(now)
		} else {
			nextStart = roundToHour(end)
			nextEnd = nextStart.Add(1 * time.Hour)
		}
	}
	return nextStart, nextEnd
}

//NextTimeFixed returns the remaining duration for the next run
func NextTimeFixed(schedType string) time.Duration {
	now := time.Now()
	if "hourly" == schedType {
		t := roundToHour(now).Add(time.Hour*1 + time.Minute*15)
		return t.Sub(now)
	}
	if "daily" == schedType {
		t := roundToDay(now).Add(time.Hour*24 + time.Minute*15)
		return t.Sub(now)
	}
	return 0
}

//NextTimeAdaptive returns an Adaptive duration.
//If the the pipeline execution time has overshot the next schedule time
//The next run is scheduled after a delay of 2 seconds
//Else it is scheduled at 15 minutes after the next hour
func NextTimeAdaptive(prevStart, prevEnd, now time.Time) time.Duration {
	nextSchedule := roundToHour(prevEnd).Add(time.Minute * 15)
	if now.Before(nextSchedule) {
		return nextSchedule.Sub(now)
	}
	return 2 * time.Second
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

func crossesDayBoundary(fd time.Time, td time.Time) bool {
	return roundToDay(td).Sub(roundToDay(fd)).Hours() > 0
}

func datePrefixGenerator(fromDate time.Time, toDate time.Time, dateFormat string) []string {
	var prefixes []string
	var dateFmt string
	if dateFormat == "" {
		dateFmt = DateFormat
	} else {
		dateFmt = dateFormat
	}
	if !crossesDayBoundary(fromDate, toDate) {
		for i := fromDate.Hour(); i < toDate.Hour(); i++ {
			str := fmt.Sprintf("%s/%02d", fromDate.Format(DateFormat), i)
			prefixes = append(prefixes, str)
		}
		return prefixes
	}

	nd := fromDate.AddDate(0, 0, 1)
	n := spanDays(roundToDay(nd), roundToDay(toDate))

	if fromDate.Hour() == 0 {
		prefixes = append(prefixes, fromDate.Format(dateFmt))
	} else {
		for i := fromDate.Hour(); i < 24; i++ {
			str := fmt.Sprintf("%s/%02d", fromDate.Format(dateFmt), i)
			prefixes = append(prefixes, str)
		}
	}

	for i := 0; i < n; i++ {
		nextDay := nd.AddDate(0, 0, i)
		prefixes = append(prefixes, nextDay.Format(dateFmt))

	}

	if toDate.Hour() != 0 {
		for i := 0; i < toDate.Hour(); i++ {
			str := fmt.Sprintf("%s/%02d", toDate.Format(dateFmt), i)
			prefixes = append(prefixes, str)
		}
	}
	return prefixes
}

//ParseDate parses a date, a date/hour or throws an error
func ParseDate(dateStr string) (time.Time, error) {
	if didMatch := dateRegex.MatchString(dateStr); didMatch {
		dt, err := time.Parse(DateFormat, dateStr)
		return dt, err
	}
	return time.Parse(DateHourFormat, dateStr)
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
		end = roundToDay(time.Now())
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
func PrefixChan(ctx context.Context, start time.Time, end time.Time, topPrefixes []string, dateFormat string) <-chan string {
	datePrefixes := datePrefixGenerator(start, end, dateFormat)
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
	return out
}
