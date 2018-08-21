package main

import (
	"flag"
	"time"

	"github.com/bamarb/aws-pipeline-go/pkg/task"
	"github.com/mediocregopher/radix.v3"
)

/* Config Types */

// Database struct to hold db conn info
type Database struct {
	Server   string
	Port     int
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
	Version   string
	Radius    string
	Output    OutputInfo
	Databases map[string]Database
}

var (
	cfgFile    string
	startTime  = time.Now()
	workerPool *task.Pool
	redisPool  *radix.Pool
	conf       Config
)

func init() {
	flag.StringVar(&cfgFile, "f", "config.toml", "# Path to cfg file (.toml)")
}

func main() {
	flag.Parse()

}
