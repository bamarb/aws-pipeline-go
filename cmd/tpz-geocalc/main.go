package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"syscall"
	"time"

	"github.com/mediocregopher/radix.v3"

	"github.com/bamarb/aws-pipeline-go/pkg/trapyz"

	"github.com/BurntSushi/toml"
	"github.com/bamarb/aws-pipeline-go/pkg/task"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

var (
	cfgFile      string
	fromDateHour string
	toDateHour   string
	startTime    = time.Now()
	config       *trapyz.Config
)

func init() {
	flag.StringVar(&cfgFile, "f", "config.toml", "# Path to cfg file (.toml)")
	flag.StringVar(&fromDateHour, "fdh", "", "From date hour in YYYY/MM/DD or YYYY/MM/DD/HH format")
	flag.StringVar(&toDateHour, "tdh", "", "To date hour in the form YYYY/MM/DD or YYYY/MM/DD/HH")
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
			log.Errorf("Error jsonify record %+v : %s", rec, err)
		}
		outFile.WriteString(string(jstr) + "\n")
	}
}

func runPipeline(ctx context.Context, config *trapyz.Config) {
	connMgr := trapyz.NewConnMgr(config)
	//log.Debugf("SQL Conn str [%s]\n", sqlConnStr)
	db := connMgr.MustConnectMysql()
	redisPool := connMgr.MustConnectRedis()
	/* Flush All */
	redisPool.Do(radix.Cmd(nil, "FLUSHALL"))
	/* Rebuild Cache */
	cache, err := trapyz.MakeCache(db, redisPool, config)
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
	/* Exec python3 GenerateDerivedAttributes.py */
	err = os.Chdir("/home/ubuntu/varunplay")
	if err != nil {
		log.Errorf("Error chdir: %s ", err)
		return
	}
	cmd := exec.Command("python3", "GenerateDerivedAttributes.py", "testDataDir/")
	err = cmd.Run()
	if err != nil {
		log.Errorf("Error Executing GenerateDerivedAttributes.py: %s", err)
	}

	cmdEs := exec.Command("python3", "ElasticSearchAnalytics.py")
	err = cmdEs.Run()
	if err != nil {
		log.Errorf("Error executing ElasticSearchAnalytics.py: %s", err)
	}
}

func main() {
	flag.Parse()
	ctx := context.Background()
	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM)
	for {
		/* Read the config */
		if _, err := toml.DecodeFile(cfgFile, &config); err != nil {
			fmt.Printf("FATAL error parsing cfg file: %s ", err)
			os.Exit(1)
		}
		configLogging(config.Output)
		nextDur := trapyz.NextTime(config.Schedule)
		nextTimer := time.NewTimer(nextDur)
		log.Infof("Next schedule : %s ", time.Now().Add(nextDur))
		select {
		case <-osSignals:
			log.Infof("Main Shutting down on user signal...")
			nextTimer.Stop()
			return
		case <-nextTimer.C:
			runPipeline(ctx, config)
		}
	}
}
