package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/bamarb/aws-pipeline-go/pkg/trapyz"

	"github.com/BurntSushi/toml"
	"github.com/bamarb/aws-pipeline-go/pkg/task"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

var (
	cfgFile   string
	startTime = time.Now()
	config    *trapyz.Config
)

func init() {
	flag.StringVar(&cfgFile, "f", "config.toml", "# Path to cfg file (.toml)")
}

func configLogging(logCfg trapyz.OutputInfo) {
	if err := os.MkdirAll(logCfg.Logdir, 0755); err != nil {
		fmt.Printf("Error creating log directory: %s", err)
		os.Exit(2)
	}
	logfile := path.Join(logCfg.Logdir, logCfg.Logfile)
	f, err := os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		fmt.Printf("Error creating log file: %s", err)
		os.Exit(2)
	}
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(f)
	log.SetLevel(log.DebugLevel)
}

func writer(records chan trapyz.GeoLocOutput, outFile *os.File) {
	for rec := range records {
		if rec.UID == "" {
			fmt.Printf("Error Rec:%+v\n", rec)
			continue
		}
		jstr, err := json.Marshal(rec)
		if err != nil {
			log.Errorf("Error jsonify record %s : %s", rec, err)
		}
		outFile.WriteString(string(jstr) + "\n")
	}
}

func main() {
	flag.Parse()
	ctx := context.Background()
	if _, err := toml.DecodeFile(cfgFile, &config); err != nil {
		fmt.Printf("FATAL error parsing cfg file : %s ", err)
		os.Exit(1)
	}
	configLogging(config.Output)
	connMgr := trapyz.NewConnMgr(config)
	//log.Debugf("SQL Conn str [%s]\n", sqlConnStr)
	db := connMgr.MustConnectMysql()
	redisPool := connMgr.MustConnectRedis()
	cache, err := trapyz.MakeCache(db, redisPool)
	if err != nil {
		redisPool.Close()
		db.Close()
		log.Fatalln(err)
	}
	/* Download S3 Files */
	s3pool := task.New(1)
	s3pool.Start()
	trapyz.S3FetchOnTimeRange(ctx, config, s3pool).Wait()
	s3pool.Stop()
	/* Start The Log Writer */
	ofile := path.Join(config.Output.Directory, config.Output.File)
	log.Infof("Creating outputfile %s", ofile)
	outPutFile, err := os.Create(ofile)
	if err != nil {
		log.Fatalf("Error unable to create outputfile:%s", err)
	}
	defer outPutFile.Close()
	outchan := make(chan trapyz.GeoLocOutput)
	go writer(outchan, outPutFile)
	/* Start the GeoStore workers and wait for them to finish */
	var nw = 4
	if config.Nworkers > 0 {
		nw = config.Nworkers
	}
	workerPool := task.New(nw)
	workerPool.Start()
	trapyz.FillGeoStores(config, cache, redisPool, workerPool, outchan).Wait()
	close(outchan)
	workerPool.Stop()
}
