package trapyz

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jmoiron/sqlx"
	"github.com/mediocregopher/radix.v3"
)

//ConnectionManager makes database connections based on config provided
type ConnectionManager struct {
	cfg *Config
}

//NewConnMgr constructs a new connection manager
func NewConnMgr(config *Config) *ConnectionManager {
	if nil == config {
		panic("Nil configuration")
	}
	return &ConnectionManager{cfg: config}
}

//MustConnectMysql make the mysql connection or die
func (cm ConnectionManager) MustConnectMysql() *sqlx.DB {
	dbCfg := cm.cfg.Db[CfgKey(cm.cfg, "mysql")]
	sqlConnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbCfg.User,
		dbCfg.Password, dbCfg.Server, dbCfg.Port, dbCfg.Dbname)
	//log.Debugf("SQL Conn str [%s]\n", sqlConnStr)
	return sqlx.MustConnect("mysql", sqlConnStr)
}

//MustConnectRedis make redis connection or die
func (cm ConnectionManager) MustConnectRedis() *radix.Pool {
	redisCfg := cm.cfg.Db[CfgKey(cm.cfg, "redis")]
	redisConnStr := fmt.Sprintf("%s:%s", redisCfg.Server, redisCfg.Port)
	redisPool, err := radix.NewPool("tcp", redisConnStr, 10)
	if err != nil {
		panic(err)
	}
	return redisPool
}

//MustConnectS3 make an S3API client or die
func (cm ConnectionManager) MustConnectS3() s3iface.S3API {
	var profile, region string
	awsCfgInfo := cm.cfg.Aws[CfgKey(cm.cfg, "s3")]
	if awsCfgInfo.Bucket == "" {
		panic(ErrorNoBucket)
	}
	if awsCfgInfo.Profile == "" {
		profile = "default"
	} else {
		profile = awsCfgInfo.Profile
	}
	if awsCfgInfo.Region == "" {
		region = "us-east-1"
	} else {
		region = awsCfgInfo.Region
	}
	//construct a session with the specified profile or die
	sess, err := session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{Region: aws.String(region)},
		Profile: profile,
	})

	if err != nil {
		panic(err)
	}
	return s3.New(sess)
}
