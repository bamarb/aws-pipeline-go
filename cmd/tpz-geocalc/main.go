package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/bamarb/aws-pipeline-go/pkg/trapyz"

	"github.com/BurntSushi/toml"
	"github.com/bamarb/aws-pipeline-go/pkg/geostore"
	"github.com/bamarb/aws-pipeline-go/pkg/task"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/mediocregopher/radix.v3"
	log "github.com/sirupsen/logrus"
)

// GeoLocCalcTask calculates the Geolocation for a bunch of files
// Each file has a bunch of json user data. Once the location is
// filled (GeoLocOutput) in, sends the output to LogWriter for processing
type GeoLocCalcTask struct {
	//A Slice of File names to process
	Files []os.FileInfo
	// redis connection pool
	RedisPool *radix.Pool
	//Send Out copies of GeoLocOutput to LogWriter
	Outchan chan trapyz.GeoLocOutput
	//A pointer to the config
	Cfg *trapyz.Config
	//The cache for lookups
	Cache *trapyz.Cache
	//Signal worker is done
	Wg *sync.WaitGroup
	//worker id
	ID int
}

//Task calculates the geo distances from a store
func (gct GeoLocCalcTask) Task() {
	defer gct.Wg.Done()
	store := geostore.NewGeoLocationStore(gct.RedisPool)
	requiredJSONKeys := []string{"apikey", "gid", "lat", "lng", "createdAt"}
	log.Infof("GeoLocCalc worker %d started processing %d files", gct.ID, len(gct.Files))
	for _, file := range gct.Files {
		var lineCount, errorCount uint
		absFile := path.Join(config.Inputdir, file.Name())
		log.Debugf("Worker %d Processing file %s", gct.ID, absFile)
		fh, err := os.OpenFile(path.Join(absFile), os.O_RDONLY, 0644)
		if err != nil {
			log.Errorf("Worker %d Error opening data file %s: %s", gct.ID, absFile, err)
			errorCount++
			continue
		}

		scanner := bufio.NewScanner(fh)
		for scanner.Scan() {
			var jsonMap map[string]interface{}
			json.Unmarshal(scanner.Bytes(), &jsonMap)
			parsedMap, ok := convertJSONMap(jsonMap, requiredJSONKeys)
			if !ok {
				log.Errorf("Error Keys Missing:%s", scanner.Text())
				errorCount++
				continue
			}
			if !gct.InputValid(parsedMap) {
				log.Errorf("Error NULL/invalid values:%s", scanner.Text())
				fh.Close()
				errorCount++
				continue
			}
			err = gct.OutputToWriter(parsedMap, store)
			if err != nil {
				errorCount++
			}
			lineCount++

		}
		fh.Close()
		log.Infof("Worker %d processed file:%s records:%d errors:%d", gct.ID, file.Name(), lineCount, errorCount)
	}
}

func convertJSONMap(jmap map[string]interface{}, requiredKeys []string) (map[string]string, bool) {
	retMap := make(map[string]string)
	for _, rkey := range requiredKeys {
		//Check if key is present
		if _, ok := jmap[rkey]; !ok {
			return nil, false
		}
		retMap[rkey] = valToString(jmap[rkey])
	}
	return retMap, true
}

//InputValid input reject invalid gids, lat, lng
func (gct GeoLocCalcTask) InputValid(vars map[string]string) bool {
	//TODO: use regexp for gid lat and lng
	if vars["gid"] == "" || vars["gid"] == "NULL" {
		return false
	}
	if vars["lat"] == "" || vars["lat"] == "NULL" {
		return false
	}
	if vars["lng"] == "" || vars["lng"] == "NULL" {
		return false
	}
	return true
}

func valToString(v interface{}) string {
	if nil == v {
		return ""
	}
	switch u := v.(type) {
	case float64:
		return strconv.FormatInt(int64(u), 10)
	case float32:
		return strconv.FormatInt(int64(u), 10)
	case int64:
		return strconv.FormatInt(int64(u), 10)
	case int32:
		return strconv.FormatInt(int64(u), 10)
	case int:
		return strconv.Itoa(u)
	case string:
		return u
	default:
		return ""
	}

}

//OutputToWriter outputs the filled GeoLocOutput struct to the writer
func (gct GeoLocCalcTask) OutputToWriter(vars map[string]string, store geostore.GeoLocationStore) error {
	var lat = vars["lat"]
	var lng = vars["lng"]
	var apik = vars["apikey"]
	var cat = vars["createdAt"]
	var gid = vars["gid"]
	radius, _ := strconv.Atoi(gct.Cfg.Radius)
	nearbyStores, err := store.NearbyWithDist(trapyz.RedisGeoIndexName, lat, lng, gct.Cfg.Radius)
	if err != nil {
		log.Errorf("Error Redis nearby-query: %s", err)
		return err
	}
	apiID := strconv.Itoa(gct.Cache.APIKeyMap[apik])
	for _, store := range nearbyStores {
		distRounded := int(store.Distance)
		if distRounded < radius {
			template, ok := gct.Cache.LocCache[store.LocID]
			if !ok {
				continue
			}
			out := trapyz.GeoLocOutput{
				UID:       store.LocID,
				Sname:     template.Sname,
				Cat:       template.Cat,
				Subcat:    template.Subcat,
				City:      template.City,
				Pin:       template.Pin,
				Apikey:    apiID,
				Lat:       lat,
				Lng:       lng,
				Gid:       gid,
				Distance:  distRounded,
				Createdat: cat,
			}
			gct.Outchan <- out
		}
	}
	return nil
}

var (
	cfgFile    string
	startTime  = time.Now()
	workerPool *task.Pool
	redisPool  *radix.Pool
	config     *trapyz.Config
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

	dbCfg := config.Db[trapyz.CfgKey(config, "mysql")]
	sqlConnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbCfg.User,
		dbCfg.Password, dbCfg.Server, dbCfg.Port, dbCfg.Dbname)
	//log.Debugf("SQL Conn str [%s]\n", sqlConnStr)
	db := sqlx.MustConnect("mysql", sqlConnStr)
	redisCfg := config.Db[trapyz.CfgKey(config, "redis")]
	redisConnStr := fmt.Sprintf("%s:%s", redisCfg.Server, redisCfg.Port)
	redisPool, err := radix.NewPool("tcp", redisConnStr, 10)
	if err != nil {
		log.Fatalln(err)
	}
	defer redisPool.Close()
	defer db.Close()
	cache, err := trapyz.MakeCache(db, redisPool)
	if err != nil {
		redisPool.Close()
		db.Close()
		log.Fatalln(err)
	}
	/* Download S3 Files */
	s3pool := task.New(4)
	s3pool.Start()
	s3wg, err := trapyz.S3Fetch(ctx, config, s3pool)
	if err != nil {
		log.Fatalf("ERROR starting S3 Fetcher:%s", err)
	}
	s3wg.Wait()

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
	/* Start the workers and wait for them to finish */
	inputDir := trapyz.FindAndCreateDestDir(config)
	config.Inputdir = inputDir
	filesToProcess, err := ioutil.ReadDir(inputDir)
	if err != nil {
		log.Fatalf("Unable to read dir [%s]: %s", inputDir, err)
	}
	/* Create and start the pool */
	var nw = 4
	var wg sync.WaitGroup

	if config.Nworkers > 0 {
		nw = config.Nworkers
	}

	workerPool = task.New(nw)
	workerPool.Start()
	var offset = len(filesToProcess) / nw
	var base = 0
	wg.Add(nw)
	for i := 0; i < nw; i++ {
		//Create the Task Objects and submit them to the pool
		if i == nw-1 {
			workerPool.Submit(GeoLocCalcTask{Files: filesToProcess[base:],
				Outchan:   outchan,
				Cache:     cache,
				ID:        i,
				RedisPool: redisPool,
				Wg:        &wg,
				Cfg:       config,
			})
		} else {
			workerPool.Submit(GeoLocCalcTask{Files: filesToProcess[base : base+offset],
				Outchan:   outchan,
				Cache:     cache,
				ID:        i,
				RedisPool: redisPool,
				Wg:        &wg,
				Cfg:       config,
			})
		}
		base = base + offset
	}
	wg.Wait()
	close(outchan)
	workerPool.Stop()
}
