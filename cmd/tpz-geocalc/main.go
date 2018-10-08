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
	config       *trapyz.Config
	runCount     = 0
	fromDateHour time.Time
	toDateHour   time.Time
)

func init() {
	flag.StringVar(&cfgFile, "f", "config.toml", "# Path to cfg file (.toml)")
}

func configLogging(logCfg trapyz.OutputInfo) *os.File {
	if err := os.MkdirAll(logCfg.Logdir, 0755); err != nil {
		fmt.Printf("Error creating log directory: %s", err)
		os.Exit(2)
	}
	logname := logCfg.Logfile + "-" + time.Now().Format("2006-01-02-15-04-05")
	logfile := path.Join(logCfg.Logdir, logname)
	f, err := os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		fmt.Printf("Error creating log file redirecting logs to /dev/null: %s", err)
		os.Exit(2)
	}
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(f)
	log.SetLevel(log.DebugLevel)
	return f
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
	log.Infoln("Rebuilding Redis store and in memory caches")
	cache, err := trapyz.MakeCache(db, redisPool, config)
	if err != nil {
		redisPool.Close()
		db.Close()
		log.Fatalln(err)
	}
	/* Download S3 Files */
	/* Calculate the time windows to down load */
	log.Infof("Starting S3 file download for range: [%s] - [%s]",
		fromDateHour.Format(time.Stamp), toDateHour.Format(time.Stamp))
	s3pool := task.New(2)
	s3pool.Start()
	trapyz.S3FetchOnRange(ctx, config, s3pool, fromDateHour, toDateHour)
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
	log.Infof("Fill Geo stores complete: [%s] - [%s]",
		fromDateHour.Format(time.Stamp), toDateHour.Format(time.Stamp))
	runExtras()
}

func runExtras() {
	if curDir, err := os.Getwd(); err == nil {
		log.Infof("current working directory:[%s]", curDir)
		defer os.Chdir(curDir)
	}

	err := os.Chdir("/home/ubuntu/varunplay")
	if err != nil {
		log.Errorf("Error chdir: %s ", err)
		return
	}
	log.Infoln("Changed working dir to :/home/ubuntu/varunplay, performing cleanup")
	outDirName := config.Output.Directory + "/"
	log.Infof("Executing python3 GenerateDerivedAttributes.py %s", outDirName)

	cmd := exec.Command("python3", "GenerateDerivedAttributes.py", outDirName)
	err = cmd.Run()
	if err != nil {
		log.Errorf("Error Executing GenerateDerivedAttributes.py: %s", err)
	}

	log.Infof("Executing python3 ElasticSearchAnalytics.py ")
	cmdEs := exec.Command("python3", "ElasticSearchAnalytics.py")
	err = cmdEs.Run()
	if err != nil {
		log.Errorf("Error executing ElasticSearchAnalytics.py: %s", err)
	}
	log.Infof("Pipeline run complete")
}

func cleanup() {
	/*
			rm -rf derive_stdin_stdout_log.log
		    rm -rf derive_stdin_stdout_log_ddb.log
			rm -rf redisgidData/*
			rm -f s3-dump/
	*/
	trapyz.CleanUpS3DumpDir(config)
	const pydir = "/home/ubuntu/varunplay/"
	key := trapyz.CfgKey(config, "redis-local")
	redisCfg := config.Db[key]
	redisConnStr := fmt.Sprintf("%s:%s", redisCfg.Server, redisCfg.Port)
	redisPool, err := radix.NewPool("tcp", redisConnStr, 1)
	if err == nil {
		log.Infof("FLUSHING local redis")
		redisPool.Do(radix.Cmd(nil, "FLUSHALL"))
		redisPool.Close()
	}
	os.Remove(pydir + "derive_stdin_stdout_log.log")
	os.Remove(pydir + "derive_stdin_stdout_log_ddb.log")
	err = os.RemoveAll(pydir + "redisgidData")
	if err != nil {
		fmt.Printf("Error removing Directory: %s\n", err)
	}
	err = os.Mkdir(pydir+"redisgidData", 0755)
	if err != nil {
		fmt.Printf("Error creating directory: %s\n", err)
	}
}

func main() {
	flag.Parse()
	ctx := context.Background()
	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM)
	var logFile *os.File
	for {
		/* Read the config */
		if _, err := toml.DecodeFile(cfgFile, &config); err != nil {
			fmt.Printf("FATAL error parsing cfg file: %s ", err)
			os.Exit(1)
		}
		if logFile != nil {
			//Close the previous log file if any
			logFile.Close()
		}
		//Cleanup data of any previous runs
		cleanup()
		//Calculate Next run date times
		fromDateHour, toDateHour = trapyz.GetStartEndTime(config.Schedule, fromDateHour, toDateHour)
		//Configure a New logfile for this run
		logFile = configLogging(config.Output)
		nextDur := trapyz.NextTimeAdaptive(fromDateHour, toDateHour, time.Now())
		log.Infof("Next schedule : %s ", time.Now().Add(nextDur))
		nextTimer := time.NewTimer(nextDur)

		select {
		case <-osSignals:
			log.Infof("Main Shutting down on user signal...")
			nextTimer.Stop()
			return
		case <-nextTimer.C:
			log.Infof("Starting Pipeline Run")
			runPipeline(ctx, config)
			log.Infof("Pipeline run ended")
		}
	}
}
