package trapyz

import (
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jmoiron/sqlx"
	"github.com/mediocregopher/radix.v3"
)

//ConnectionHolder holds the connection manager singleton
var connectionHolder *ConnectionManager
var once sync.Once

//ConnectionManager makes database connections based on config provided
type ConnectionManager struct {
	lock      sync.RWMutex
	cfg       *Config
	connCache map[string]interface{}
}

//NewConnMgr constructs a new connection manager
func NewConnMgr(config *Config) *ConnectionManager {
	if nil == config {
		return nil
	}
	once.Do(func() {
		connectionHolder = &ConnectionManager{cfg: config, connCache: make(map[string]interface{})}
	})
	return connectionHolder
}

//MustConnectMysql make the mysql connection or die
func (cm *ConnectionManager) MustConnectMysql() *sqlx.DB {
	key := CfgKey(cm.cfg, "mysql")
	cm.lock.RLock()
	if ret, present := cm.connCache[key]; present {
		cm.lock.RUnlock()
		return ret.(*sqlx.DB)
	}
	cm.lock.RUnlock()
	cm.lock.Lock()
	dbCfg := cm.cfg.Db[key]
	sqlConnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbCfg.User,
		dbCfg.Password, dbCfg.Server, dbCfg.Port, dbCfg.Dbname)
	//log.Debugf("SQL Conn str [%s]\n", sqlConnStr)
	conn := sqlx.MustConnect("mysql", sqlConnStr)
	cm.connCache[key] = conn
	cm.lock.Unlock()
	return conn
}

//MustConnectRedis make redis connection or die
func (cm *ConnectionManager) MustConnectRedis() *radix.Pool {
	key := CfgKey(cm.cfg, "redis")
	cm.lock.RLock()
	if conn, present := cm.connCache[key]; present {
		cm.lock.RUnlock()
		return conn.(*radix.Pool)
	}
	cm.lock.RUnlock()
	cm.lock.Lock()
	redisCfg := cm.cfg.Db[key]
	redisConnStr := fmt.Sprintf("%s:%s", redisCfg.Server, redisCfg.Port)
	redisPool, err := radix.NewPool("tcp", redisConnStr, 10)
	if err != nil {
		cm.lock.Unlock()
		panic(err)
	}
	cm.connCache[key] = redisPool
	cm.lock.Unlock()
	return redisPool
}

//MustConnectS3 make an S3API client or die
func (cm *ConnectionManager) MustConnectS3() s3iface.S3API {
	key := CfgKey(cm.cfg, "s3")
	cm.lock.RLock()
	if conn, present := cm.connCache[key]; present {
		cm.lock.RUnlock()
		return conn.(s3iface.S3API)
	}
	cm.lock.RUnlock()
	var profile, region string
	awsCfgInfo := cm.cfg.Aws[key]
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
	cm.lock.Lock()
	//construct a session with the specified profile or die
	sess, err := session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{Region: aws.String(region)},
		Profile: profile,
	})

	if err != nil {
		cm.lock.Unlock()
		panic(err)
	}
	conn := s3.New(sess)
	cm.connCache[key] = conn
	cm.lock.Unlock()
	return conn
}
