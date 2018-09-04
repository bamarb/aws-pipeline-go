package trapyz

import (
	"strconv"

	"github.com/bamarb/aws-pipeline-go/pkg/geostore"
	"github.com/jmoiron/sqlx"
	radix "github.com/mediocregopher/radix.v3"
	log "github.com/sirupsen/logrus"
)

//RedisGeoIndexName the index key in redis where the cache is stored
const RedisGeoIndexName = "store:locations"

//Cache holds reverse index maps for reverse lookup
type Cache struct {
	APIKeyMap map[string]int
	CatMap    map[string]int
	ScatMap   map[string]int
	CityMap   map[string]int
	PinMap    map[int]int
	LocCache  map[string]GeoLocOutput
}

//MakeCache a utility function that populates the cache
func MakeCache(db *sqlx.DB, redisPool *radix.Pool) (*Cache, error) {
	log.Debugln("Populating redis geo cache")
	err := populateRedisGeoData(db, redisPool)
	if nil != err {
		return nil, err
	}
	/* Populate Caches */
	aKeyMap := mkAPIKeyMap(db)
	catMap := mkCategoryMap(db)
	subCatMap := mkSubCategoryMap(db)
	cityMap := mkCityMap(db)
	pinMap := mkPincodeMap(db)
	locCache := mkLocCache(db, catMap, subCatMap, cityMap, pinMap)
	log.Debugf("Geo Location cache populated with %d keys", len(locCache))
	return &Cache{aKeyMap, catMap, subCatMap, cityMap, pinMap, locCache}, nil
}

// Reads store data from mysql and creates a geo index in redis
// TODO: Create a Batch Uploader to save RTT
func populateRedisGeoData(db *sqlx.DB, rp *radix.Pool) error {
	const redisGeoIndexName = "store:locations"
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

func mkAPIKeyMap(db *sqlx.DB) map[string]int {
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
