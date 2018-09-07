package trapyz

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestPrefixChan(t *testing.T) {
	bctx := context.Background()
	topPfx1 := []string{"bobble"}
	yesterday := roundToDay(time.Now().AddDate(0, 0, -1))
	prefixYesterday := fmt.Sprintf("bobble/%s", yesterday.Format(dateFormat))
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

		{"Non Neg Test 5", args{bctx, "", "", topPfx1},
			[]string{prefixYesterday}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PrefixChan(tt.args.ctx, tt.args.fromDate, tt.args.toDate, tt.args.topPrefixes)
			if (err != nil) != tt.wantErr {
				t.Errorf("PrefixChan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if nil == got {
				t.Errorf("PrefixChan() unexpected nil channel but error = %v", err)
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
