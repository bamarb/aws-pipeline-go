package trapyz

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"
)

func timeFor(dateTime string) time.Time {
	time, err := ParseDate(dateTime)
	if err != nil {
		log.Fatal(err)
	}
	return time
}

func TestPrefixChan(t *testing.T) {
	bctx := context.Background()
	topPfx1 := []string{"bobble"}
	yesterday := roundToDay(time.Now().AddDate(0, 0, -1))
	prefixYesterday := fmt.Sprintf("bobble/%s", yesterday.Format(dateFormat))
	dateStart, dateEnd, err := ParseDates("", "")
	if err != nil {
		t.Error("ParseDates unexpected error:", err)
	}

	type args struct {
		ctx         context.Context
		fromDate    string
		toDate      string
		topPrefixes []string
	}

	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{"Non Neg Test 0", args{bctx, "2018/01/01", "2018/01/02", topPfx1},
			[]string{"bobble/2018/01/01"}, false},

		{"Non Neg Test 1", args{bctx, "2018/01/01/23", "2018/01/02/01", topPfx1},
			[]string{"bobble/2018/01/01/23", "bobble/2018/01/02/00"}, false},

		{"Non Neg Test 2", args{bctx, "2018/01/01/23", "2018/01/02/00", topPfx1},
			[]string{"bobble/2018/01/01/23"}, false},

		{"Non Neg Test 4", args{bctx, "2018/09/06/00", "2018/09/06/02", topPfx1},
			[]string{"bobble/2018/09/06/00", "bobble/2018/09/06/01"}, false},

		{"Non Neg Test 5", args{bctx, dateStart.Format(dateHourFormat), dateEnd.Format(dateHourFormat), topPfx1},
			[]string{prefixYesterday}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PrefixChan(tt.args.ctx, timeFor(tt.args.fromDate), timeFor(tt.args.toDate), tt.args.topPrefixes)
			if nil == got {
				t.Errorf("PrefixChan() unexpected nil channel")
				return
			}

			for pfx := range got {
				if tt.want == nil {
					t.Logf("Got prefix:%s", pfx)
					continue
				}
				found := false
				for _, expectedStr := range tt.want {
					if expectedStr == pfx {
						found = true
					}
				}
				if !found {
					t.Errorf("Expected prefix not found:[%s]", pfx)
				}
			}
		})
	}
}

func TestNextTimeAdaptive(t *testing.T) {
	type args struct {
		prevStart time.Time
		prevEnd   time.Time
		now       time.Time
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{"Test Basic Case",
			args{timeFor("2018/01/01/00"), timeFor("2018/01/01/01"), timeFor("2018/01/01/01").Add(5 * time.Minute)},
			time.Minute * 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NextTimeAdaptive(tt.args.prevStart, tt.args.prevEnd, tt.args.now); got != tt.want {
				t.Errorf("NextTimeAdaptive() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetStartEndTime(t *testing.T) {
	var zeroStart, zeroEnd time.Time
	type args struct {
		schedType string
		start     time.Time
		end       time.Time
	}
	tests := []struct {
		name  string
		args  args
		want  time.Time
		want1 time.Time
	}{
		{"Test-Hourly-Zero-Start-End",
			args{"hourly", zeroStart, zeroEnd},
			roundToHour(time.Now()).Add(-1 * time.Hour), roundToHour(time.Now()),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetStartEndTime(tt.args.schedType, tt.args.start, tt.args.end)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetStartEndTime() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("GetStartEndTime() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
