package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/bamarb/aws-pipeline-go/pkg/geostore"
	"github.com/bamarb/aws-pipeline-go/pkg/task"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
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

var (
	cfgFile    string
	startTime  = time.Now()
	workerPool *task.Pool
	redisPool  *radix.Pool
	config     *Config
)

const (
	//The Key Name for store locations
	redisGeoIndexName = "store:locations"
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
	pm map[int]int) map[string]*GeoLocOutput {
	q := `SELECT s.Store_ID AS uuid, m.sname , m.cat , m.subcat, m.city, m.pincode
	  FROM StoreUuidMap s INNER JOIN MasterRecordSet m ON s.Store_Uuid = m.UUID`
	rows, err := db.Queryx(q)
	if err != nil {
		log.Errorf("mkLocCache query failed: %s", err)
	}
	defer rows.Close()
	ret := make(map[string]*GeoLocOutput, 45000)

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
		ret[uuidstr] = &GeoLocOutput{UID: uuidstr, Pin: pinid, Sname: sname, Cat: catid, Subcat: subcatid, City: cityid}
	}
	return ret
}

func main() {
	flag.Parse()
	var catMap, subCatMap, cityMap map[string]int
	var pinMap map[int]int
	var locCache map[string]*GeoLocOutput

	if _, err := toml.DecodeFile(cfgFile, &config); err != nil {
		fmt.Printf("FATAL error parsing cfg file : %s ", err)
		os.Exit(1)
	}
	configLogging(config.Output)

	//TODO: Pull this from config or ENV
	dbCfg := config.Db["mysql-dev"]
	sqlConnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbCfg.User,
		dbCfg.Password, dbCfg.Server, dbCfg.Port, dbCfg.Dbname)
	log.Debugf("SQL Conn str [%s]\n", sqlConnStr)
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
	catMap = mkCategoryMap(db)
	subCatMap = mkSubCategoryMap(db)
	cityMap = mkCityMap(db)
	pinMap = mkPincodeMap(db)
	locCache = mkLocCache(db, catMap, subCatMap, cityMap, pinMap)
	log.Debugf("Geo Location cache populated with %d keys", len(locCache))

}
