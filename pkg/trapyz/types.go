package trapyz

import (
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// GeoLocOutput the structure we will marshal to json and write
// to log
type GeoLocOutput struct {
	Pin       string `json:"pin"`
	Gid       string `json:"gid"`
	Lat       string `json:"lat"`
	UID       string `json:"uuid"`
	Sname     string `json:"sname"`
	Cat       string `json:"cat"`
	Apikey    string `json:"apikey"`
	Lng       string `json:"lng"`
	Subcat    string `json:"subcat"`
	Distance  int    `json:"distance"`
	City      string `json:"city"`
	Createdat string `json:"createdat"`
}

//S3Downloader downloads files from trapyz s3 buckets to a directory specified
type S3Downloader struct {
	client   s3iface.S3API
	bucket   string
	prefix   string
	dateFrom time.Time
	dateTo   time.Time
	destDir  string
	flatten  bool
	unzip    bool
}
