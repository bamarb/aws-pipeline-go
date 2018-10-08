package trapyz

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"

	"github.com/bamarb/aws-pipeline-go/pkg/geostore"
	"github.com/bamarb/aws-pipeline-go/pkg/task"
	radix "github.com/mediocregopher/radix.v3"
	log "github.com/sirupsen/logrus"
)

// GeoLocCalcTask calculates the Geolocation for a bunch of files
// Each file has a bunch of json user data. Once the location is
// filled (GeoLocOutput) in, sends the output to LogWriter for processing
type GeoLocCalcTask struct {
	//A channel providing File names to process
	Inchan chan string
	// redis connection pool
	RedisPool *radix.Pool
	//Send Out copies of GeoLocOutput to LogWriter
	Outchan chan GeoLocOutput
	//A pointer to the config
	Cfg *Config
	//The cache for lookups
	Cache *Cache
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
	for file := range gct.Inchan {
		var lineCount, errorCount uint
		absFile := path.Join(gct.Cfg.Inputdir, file)
		log.Debugf("Worker %d processing file %s", gct.ID, absFile)
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
				//log.Errorf("Error Keys Missing:%s", scanner.Text())
				errorCount++
				continue
			}
			if !gct.InputValid(parsedMap) {
				//log.Errorf("Error NULL/invalid values:%s", scanner.Text())
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
		log.Infof("Worker %d processed file:%s records:%d errors:%d", gct.ID, file, lineCount, errorCount)
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
	nearbyStores, err := store.NearbyWithDist(RedisGeoIndexName, lat, lng, gct.Cfg.Radius)
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

//FillGeoStores fills nearby stores based on Lat,Long data
func FillGeoStores(config *Config, cache *Cache, redisPool *radix.Pool,
	taskPool *task.Pool, outchan chan GeoLocOutput) *sync.WaitGroup {
	var wg sync.WaitGroup
	inputDir := FindOrCreateDestDir(config)
	config.Inputdir = inputDir
	filesToProcess, err := ioutil.ReadDir(inputDir)
	if err != nil {
		log.Fatalf("Unable to read dir [%s]: %s", inputDir, err)
	}
	if len(filesToProcess) == 0 {
		return &wg
	}
	inChan := make(chan string)
	var nw = 4
	if config.Nworkers > 0 {
		nw = config.Nworkers
	}
	go func() {
		for _, file := range filesToProcess {
			inChan <- file.Name()
		}
		close(inChan)
	}()
	wg.Add(nw)
	for i := 0; i < nw; i++ {
		taskPool.Submit(GeoLocCalcTask{Inchan: inChan,
			Outchan:   outchan,
			Cache:     cache,
			ID:        i,
			RedisPool: redisPool,
			Wg:        &wg,
			Cfg:       config,
		})
	}
	return &wg
}
