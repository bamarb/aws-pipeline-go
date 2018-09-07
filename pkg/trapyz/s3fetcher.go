package trapyz

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/bamarb/aws-pipeline-go/pkg/file"
	"github.com/bamarb/aws-pipeline-go/pkg/task"
	log "github.com/sirupsen/logrus"
)

//ErrorNoBucket is thrown when bucket cannot be found in config
var ErrorNoBucket = errors.New("Error no aws bucket")

//S3Fetch starts a s3 fetcher
func S3Fetch(ctx context.Context, cfg *Config, taskPool *task.Pool) (*sync.WaitGroup, error) {
	var wg sync.WaitGroup
	var profile, region string
	awsCfgInfo := cfg.Aws[CfgKey(cfg, "s3")]
	if awsCfgInfo.Bucket == "" {
		return nil, ErrorNoBucket
	}
	if awsCfgInfo.Profile == "" {
		profile = "default"
	} else {
		profile = awsCfgInfo.Profile
	}
	if awsCfgInfo.Region == "" {
		region = "us-east-1"
	} else {
		region = awsCfgInfo.Region
	}
	//Parse The dates and get the channel
	prefixChan, err := PrefixChan(ctx, awsCfgInfo.DateFrom,
		awsCfgInfo.DateTo, awsCfgInfo.Prefixes)

	if err != nil {
		return nil, err
	}
	//Get the Dump Prefix
	dumpDir := FindAndCreateDestDir(cfg)
	//construct a session with the specified profile or die
	sess, err := session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{Region: aws.String(region)},
		Profile: profile,
	})

	if err != nil {
		return nil, err
	}
	s3c := s3.New(sess)
	for i := 0; i < cfg.Nworkers; i++ {
		task := &S3FetcherTask{ctx, s3c, awsCfgInfo.Bucket, prefixChan, dumpDir, true, &wg}
		taskPool.Submit(task)
		wg.Add(1)
	}

	return &wg, nil
}

//S3FetcherTask is a task that downloads  S3 objects
// The object Prefixes are supplied to the task over the prefix channel
type S3FetcherTask struct {
	ctx        context.Context
	conn       s3iface.S3API
	bucket     string
	prefixChan <-chan string
	dumpDir    string
	flatten    bool
	//Signal worker is done
	wg *sync.WaitGroup
}

//Task the task the fetcter executes
func (ft *S3FetcherTask) Task() {
	for prefix := range ft.prefixChan {
		//List the Files(Objects) for the prefix
		s3files := ft.filesForPrefix(prefix)
		for _, s3file := range s3files {
			log.Infof("Downloading  file:%s  size:%d", s3file, s3file.Size())
			err := s3file.Download(ft.ctx, ft.dumpDir)
			if err != nil {
				log.Errorf("ERROR downloading file: %s", s3file.Relative())
			}
		}
	}
	ft.wg.Done()
}

func (ft *S3FetcherTask) filesForPrefix(pfx string) []file.File {
	s3files := make([]file.File, 0, 5)
	s3Input := s3.ListObjectsV2Input{
		Bucket: aws.String(ft.bucket),
		Prefix: aws.String(pfx),
	}

	err := ft.conn.ListObjectsV2PagesWithContext(ft.ctx, &s3Input,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, obj := range page.Contents {
				s3files = append(s3files, file.NewS3File(ft.conn, ft.bucket, obj))
			}
			return true
		})
	if err != nil {
		log.Errorf("Error S3Fetcher listing objects: %s", err)
		return nil
	}
	return s3files
}
