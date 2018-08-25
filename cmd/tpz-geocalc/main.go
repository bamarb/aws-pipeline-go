package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/bamarb/aws-pipeline-go/pkg/geostore"
	"github.com/bamarb/aws-pipeline-go/pkg/task"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/mediocregopher/radix.v3"
	log "github.com/sirupsen/logrus"
)

const (
	//The Key Name for store locations
	redisGeoIndexName = "store:locations"
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
	Version  string
	Radius   string
	Nworkers int
	Inputdir string
	TpzEnv   string
	Output   OutputInfo
	Db       map[string]Database
}

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

// GeoLocCalcTask calculates the Geolocation for a bunch of files
// Each file has a bunch of json user data. Once the location is
// filled (GeoLocOutput) in, sends the output to LogWriter for processing
type GeoLocCalcTask struct {
	//A Slice of File names to process
	Files []os.FileInfo
	// redis connection pool
	RedisPool *radix.Pool
	//Send Out copies of GeoLocOutput to LogWriter
	Outchan chan GeoLocOutput
	//A pointer to the config
	Cfg *Config
	//API Key Map
	APIKeyMap map[string]int
	//The cache for lookups
	Cache map[string]GeoLocOutput
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
	nearbyStores, err := store.NearbyWithDist(redisGeoIndexName, lat, lng, gct.Cfg.Radius)
	if err != nil {
		log.Errorf("Error Redis nearby-query: %s", err)
		return err
	}
	apiID := strconv.Itoa(gct.APIKeyMap[apik])
	for _, store := range nearbyStores {
		distRounded := int(store.Distance)
		if distRounded < radius {
			template, ok := gct.Cache[store.LocID]
			if !ok {
				continue
			}
			out := GeoLocOutput{
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

// Reads store data from mysql and creates a geo index in redis
// TODO: Create a Batch Uploader to save RTT
func populateRedisGeoData(db *sqlx.DB, rp *radix.Pool) error {
	query := `SELECT StoreUuidMap.Store_ID, MasterRecordSet.lat , MasterRecordSet.lng 
	 FROM StoreUuidMap INNER JOIN MasterRecordSet 
	 ON StoreUuidMap.Store_Uuid = MasterRecordSet.UUID ORDER BY StoreUuidMap.Store_ID;
`
	rows, err := db.Query(query)
	if nil != err {
		return err
	}
	defer rows.Close()
	redisStore := geostore.NewGeoLocationStore(rp)
	results := make([]string, 45000)
	for rows.Next() {
		var id, lat, lng string
		err = rows.Scan(&id, &lat, &lng)
		if nil != err {
			log.Errorf("Row scan error:%s\n", err)
			return err
		}
		results = append(results, lng, lat, id)
		_, err = redisStore.AddOrUpdateLocations(redisGeoIndexName, lng, lat, id)
		if nil != err {
			log.Errorf("Redis store error [id:%s,lng:%s,lat:%s] :%s\n", id, lng, lat, err)
		}
	}
	return nil
}

func mkCategoryMap(db *sqlx.DB) map[string]int {
	ret := make(map[string]int)
	rows, err := db.Query(`SELECT * from CategoryMap`)
	if err != nil {
		return ret
	}
	defer rows.Close()
	for rows.Next() {
		var catid int
		var name string
		err = rows.Scan(&catid, &name)
		if nil != err {
			log.Errorf("Error scan CategoryMap: %s", err)
			continue
		}
		ret[name] = catid
	}
	return ret
}

// Maps name to sub cat id
func mkSubCategoryMap(db *sqlx.DB) map[string]int {
	ret := make(map[string]int)
	rows, err := db.Query(`SELECT * from SubCategoryMap`)
	if err != nil {
		return ret
	}
	defer rows.Close()
	for rows.Next() {
		var catid, subcatid int
		var name string
		err = rows.Scan(&catid, &subcatid, &name)
		if nil != err {
			log.Errorf("Error scan SubCategoryMap: %s", err)
			continue
		}
		ret[name] = subcatid
	}
	return ret
}

func mkApiKeyMap(db *sqlx.DB) map[string]int {
	ret := make(map[string]int)
	rows, err := db.Query(`SELECT * from ApikeyMap`)
	if err != nil {
		return ret
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if nil != err {
			log.Errorf("Error scan ApikeyMap: %s", err)
			continue
		}
		ret[name] = id
	}
	return ret
}

func mkCityMap(db *sqlx.DB) map[string]int {
	ret := make(map[string]int)
	rows, err := db.Query(`SELECT * from CityMap`)
	if err != nil {
		return ret
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if nil != err {
			log.Errorf("Error scan CityMap: %s", err)
			continue
		}
		ret[name] = id
	}
	return ret
}

func mkPincodeMap(db *sqlx.DB) map[int]int {
	ret := make(map[int]int)
	rows, err := db.Query(`SELECT id, Pincode from Pincode`)
	if err != nil {
		return ret
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name int
		err = rows.Scan(&id, &name)
		if nil != err {
			log.Errorf("Error scan Pincode: %s", err)
			continue
		}
		ret[name] = id
	}
	return ret
}

func mkLocCache(db *sqlx.DB, catm, scatm, cm map[string]int,
	pm map[int]int) map[string]GeoLocOutput {
	q := `SELECT s.Store_ID AS uuid, m.sname , m.cat , m.subcat, m.city, m.pincode
	  FROM StoreUuidMap s INNER JOIN MasterRecordSet m ON s.Store_Uuid = m.UUID`
	rows, err := db.Queryx(q)
	if err != nil {
		log.Errorf("mkLocCache query failed: %s", err)
	}
	defer rows.Close()
	ret := make(map[string]GeoLocOutput, 45000)

	for rows.Next() {
		var uuid, pincode int
		var sname, cat, subcat, city string
		var catid, subcatid, cityid, pinid string
		err = rows.Scan(&uuid, &sname, &cat, &subcat, &city, &pincode)
		if nil != err {
			log.Errorf("mkLocCache scan failed: %s\n", err)
			continue
		}
		//log.Debugf("Scanned: %d, %s, %s, %s,%s, %d", uuid, sname, cat, subcat, city, pincode)
		uuidstr := strconv.Itoa(uuid)
		catid = strconv.Itoa(catm[cat])
		subcatid = strconv.Itoa(scatm[subcat])
		cityid = strconv.Itoa(cm[city])
		pinid = strconv.Itoa(pm[pincode])
		ret[uuidstr] = GeoLocOutput{UID: uuidstr, Pin: pinid, Sname: sname, Cat: catid, Subcat: subcatid, City: cityid}
	}
	return ret
}

func writer(records chan GeoLocOutput, outFile *os.File) {
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
	var aKeyMap, catMap, subCatMap, cityMap map[string]int
	var pinMap map[int]int
	var locCache map[string]GeoLocOutput

	if _, err := toml.DecodeFile(cfgFile, &config); err != nil {
		fmt.Printf("FATAL error parsing cfg file : %s ", err)
		os.Exit(1)
	}
	configLogging(config.Output)

	//TODO: Pull this from config or ENV
	dbCfg := config.Db["mysql-dev"]
	sqlConnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbCfg.User,
		dbCfg.Password, dbCfg.Server, dbCfg.Port, dbCfg.Dbname)
	//log.Debugf("SQL Conn str [%s]\n", sqlConnStr)
	db := sqlx.MustConnect("mysql", sqlConnStr)
	//TODO: use config
	redisPool, err := radix.NewPool("tcp", "localhost:6379", 10)
	if err != nil {
		log.Fatalln(err)
	}
	defer redisPool.Close()
	defer db.Close()
	log.Debugln("Populating redis geo cache")
	err = populateRedisGeoData(db, redisPool)
	if nil != err {
		log.Fatalln(err)
	}
	/* Populate Caches */
	aKeyMap = mkApiKeyMap(db)
	catMap = mkCategoryMap(db)
	subCatMap = mkSubCategoryMap(db)
	cityMap = mkCityMap(db)
	pinMap = mkPincodeMap(db)
	locCache = mkLocCache(db, catMap, subCatMap, cityMap, pinMap)
	log.Debugf("Geo Location cache populated with %d keys", len(locCache))
	/* Start The Log Writer */
	ofile := path.Join(config.Output.Directory, config.Output.File)
	log.Infof("Creating outputfile %s", ofile)
	outPutFile, err := os.Create(ofile)
	if err != nil {
		log.Fatalf("Error unable to create outputfile:%s", err)
	}
	defer outPutFile.Close()
	outchan := make(chan GeoLocOutput)
	go writer(outchan, outPutFile)
	/* Start the workers and wait for them to finish */
	filesToProcess, err := ioutil.ReadDir(config.Inputdir)
	if err != nil {
		log.Fatalf("Unable to read dir [%s]: %s", config.Inputdir, err)
	}
	/* Create and start the pool */
	var nw = 1
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
				APIKeyMap: aKeyMap,
				Cache:     locCache,
				ID:        i,
				RedisPool: redisPool,
				Wg:        &wg,
				Cfg:       config,
			})
		} else {
			workerPool.Submit(GeoLocCalcTask{Files: filesToProcess[base : base+offset],
				Outchan:   outchan,
				APIKeyMap: aKeyMap,
				Cache:     locCache,
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
