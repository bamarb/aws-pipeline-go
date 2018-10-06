package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
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
	skipS3       bool
)

func init() {
	flag.StringVar(&cfgFile, "f", "config.toml", "# Path to cfg file (.toml)")
	flag.StringVar(&fromDateHour, "fdh", "", "From date hour in YYYY/MM/DD or YYYY/MM/DD/HH format")
	flag.StringVar(&toDateHour, "tdh", "", "To date hour in the form YYYY/MM/DD or YYYY/MM/DD/HH")
	flag.BoolVar(&skipS3, "s", false, "Skip make cache and s3 download steps")
}

func cleanup() {
	/*
			rm -rf derive_stdin_stdout_log.log
		    rm -rf derive_stdin_stdout_log_ddb.log
		    rm -rf redisgidData/*
	*/
	cwd, _ := os.Getwd()
	key := trapyz.CfgKey(config, "redis-local")
	redisCfg := config.Db[key]
	redisConnStr := fmt.Sprintf("%s:%s", redisCfg.Server, redisCfg.Port)
	redisPool, err := radix.NewPool("tcp", redisConnStr, 1)
	if err == nil {
		fmt.Printf("FLUSHING local redis")
		redisPool.Do(radix.Cmd(nil, "FLUSHALL"))
		redisPool.Close()
	}
	fmt.Printf("Cleanup: Current working directory [%s]\n", cwd)
	os.Remove("derive_stdin_stdout_log.log")
	os.Remove("derive_stdin_stdout_log_ddb.log")
	err = os.RemoveAll("redisgidData")
	if err != nil {
		fmt.Printf("Error removing Directory: %s\n", err)
	}
	err = os.Mkdir("redisgidData", 0755)
	if err != nil {
		fmt.Printf("Error creating directory: %s\n", err)
	}
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

func validateAndSetDates(cfg *trapyz.Config) {
	if _, err := trapyz.ParseDate(fromDateHour); err != nil {
		fmt.Printf("Error Parsing start date: %s", err)
		os.Exit(1)
	}

	if _, err := trapyz.ParseDate(toDateHour); err != nil {
		fmt.Printf("Error Parsing start date: %s", err)
		os.Exit(1)
	}
	dbKey := trapyz.CfgKey(cfg, "s3")
	awsCfgInfo := cfg.Aws[dbKey]
	awsCfgInfo.DateFrom = fromDateHour
	awsCfgInfo.DateTo = toDateHour
	cfg.Aws[dbKey] = awsCfgInfo
}

func runPipeline(ctx context.Context, config *trapyz.Config) {
	if !skipS3 {
		fmt.Printf("%s :Run Pipeline started\n", time.Now())
		connMgr := trapyz.NewConnMgr(config)
		var nw = 4
		if config.Nworkers > 0 {
			nw = config.Nworkers
		}
		//log.Debugf("SQL Conn str [%s]\n", sqlConnStr)
		db := connMgr.MustConnectMysql()
		redisPool := connMgr.MustConnectRedis()
		/* Flush All */
		redisPool.Do(radix.Cmd(nil, "FLUSHALL"))
		/* Rebuild Cache */
		fmt.Printf("%s :Populating Redis Cache\n", time.Now())
		cache, err := trapyz.MakeCache(db, redisPool, config)
		if err != nil {
			redisPool.Close()
			db.Close()
			log.Fatalln(err)
		}
		/* Download S3 Files */
		s3pool := task.New(1)
		s3pool.Start()
		dbKey := trapyz.CfgKey(config, "s3")
		fmt.Printf("%s :Fetching S3 Files from %s to %s\n", time.Now(), config.Aws[dbKey].DateFrom, config.Aws[dbKey].DateTo)
		trapyz.S3FetchOnTimeRange(ctx, config, s3pool).Wait()
		s3pool.Stop()
		/* Start The Log Writer */
		os.MkdirAll(config.Output.Directory, 0755)
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
		workerPool := task.New(nw)
		workerPool.Start()
		fmt.Printf("%s :Processing json filling Geo stores data\n", time.Now())
		trapyz.FillGeoStores(config, cache, redisPool, workerPool, outchan).Wait()
		close(outchan)
		workerPool.Stop()
	}
	runExtras()
	log.Infof("Pipeline Execution Completed")
}

func runExtras() {
	fmt.Printf("%s:Populating dynamo DB and elasticsearch\n", time.Now())
	if curDir, err := os.Getwd(); err == nil {

		log.Infof("current working directory:[%s]", curDir)
		defer os.Chdir(curDir)
	}

	err := os.Chdir("/home/ubuntu/varunplay")
	if err != nil {
		log.Errorf("Error chdir: %s ", err)
		return
	}
	fmt.Println("Changed working dir to :/home/ubuntu/varunplay")
	cleanup()
	outDirName := config.Output.Directory + "/"
	fmt.Println("Executing python3 GenerateDerivedAttributes.py ", outDirName)

	cmd := exec.Command("python3", "GenerateDerivedAttributes.py", outDirName)
	err = cmd.Run()
	if err != nil {
		log.Errorf("Error Executing GenerateDerivedAttributes.py: %s", err)
	}

	fmt.Println("Executing python3 ElasticSearchAnalytics.py ")
	cmdEs := exec.Command("python3", "ElasticSearchAnalytics.py")
	err = cmdEs.Run()
	if err != nil {
		log.Errorf("Error executing ElasticSearchAnalytics.py: %s", err)
	}

}

func main() {
	flag.Parse()
	ctx := context.Background()
	var logFile *os.File
	/* Read the config */
	if _, err := toml.DecodeFile(cfgFile, &config); err != nil {
		fmt.Printf("FATAL error parsing cfg file: %s ", err)
		os.Exit(1)
	}
	validateAndSetDates(config)
	if logFile != nil {
		//Close the previous log file if any
		logFile.Close()
	}
	//Configure a New logfile for this run
	logFile = configLogging(config.Output)
	runPipeline(ctx, config)
	logFile.Close()
}
