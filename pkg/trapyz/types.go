package trapyz

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

// Database struct to hold db conn info
type Database struct {
	Server   string
	Port     string
	Dbname   string
	User     string
	Password string
	Tables   DbTableName
}

//DbTableName holds table names to query
type DbTableName struct {
	MasterRecTable string `toml:"master_rec_table"`
	StoreUUIDTable string `toml:"store_uuid_table"`
	PincodeTable   string `toml:"pincode_table"`
	CityTable      string `toml:"city_table"`
}

// OutputInfo struct to write output files and logs
type OutputInfo struct {
	Directory string
	File      string
	Logdir    string
	Logfile   string
	Redisdir  string
}

// AwsS3Info holds aws s3 config
type AwsS3Info struct {
	Region       string
	Profile      string
	Bucket       string
	Prefixes     []string
	DateFrom     string `toml:"date_from"`
	DateTo       string `toml:"date_to"`
	S3dumpPrefix string `toml:"s3dump_prefix"`
	Flatten      bool
	Unzip        bool
}

// Config config struct decoded from toml
type Config struct {
	Version    string
	Radius     string
	Nworkers   int
	Inputdir   string
	Schedule   string
	TpzEnv     string `toml:"tpz_env"`
	NumRecords int    `toml:"num_records"`
	Output     OutputInfo
	Db         map[string]Database
	Aws        map[string]AwsS3Info
}
