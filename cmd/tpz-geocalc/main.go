package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/bamarb/aws-pipeline-go/pkg/task"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mediocregopher/radix.v3"
	log "github.com/sirupsen/logrus"
)

/* Config Types */

// Database struct to hold db conn info
type Database struct {
	Server   string
	Port     string
	Dbname   string
	User     string
	Password string
}

// OutputInfo struct to write output files and logs
type OutputInfo struct {
	Directory string
	File      string
	Logdir    string
	Logfile   string
}

// Config config struct decoded from toml
type Config struct {
	Version string
	Radius  string
	Output  OutputInfo
	Db      map[string]Database
}

var (
	cfgFile    string
	startTime  = time.Now()
	workerPool *task.Pool
	redisPool  *radix.Pool
	config     *Config
)

func init() {
	flag.StringVar(&cfgFile, "f", "config.toml", "# Path to cfg file (.toml)")
}

func configLogging(logCfg OutputInfo) {
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

func main() {
	flag.Parse()
	if _, err := toml.DecodeFile(cfgFile, &config); err != nil {
		fmt.Printf("FATAL error parsing cfg file : %s ", err)
		os.Exit(1)
	}
	configLogging(config.Output)

	// Todo Pull this from config or ENV
	dbCfg := config.Db["mysql-prod"]
	sqlConnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbCfg.User,
		dbCfg.Password, dbCfg.Server, dbCfg.Port, dbCfg.Dbname)

	db, err := sql.Open("mysql", sqlConnStr)
	if err != nil {
		log.Error(err)
		os.Exit(3)
	}
	defer db.Close()

}
