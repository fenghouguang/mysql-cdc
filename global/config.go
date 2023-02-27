package global

import (
	"io/ioutil"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/juju/errors"
	"gopkg.in/yaml.v2"

	"go-mysql-cdc/util/files"
	"go-mysql-cdc/util/logs"
	"go-mysql-cdc/util/sys"
)

const (
	_targetRedis         = "REDIS"
	_targetMongodb       = "MONGODB"
	_targetRocketmq      = "ROCKETMQ"
	_targetRabbitmq      = "RABBITMQ"
	_targetKafka         = "KAFKA"
	_targetElasticsearch = "ELASTICSEARCH"
	_targetScript        = "SCRIPT"
	_targetMysql         = "MYSQL"

	RedisGroupTypeSentinel = "sentinel"
	RedisGroupTypeCluster  = "cluster"

	_dataDir = "store"

	_zkRootDir = "/transfer" // ZooKeeper and Etcd root

	_flushBulkInterval = 200
	_flushBulkSize     = 100

	// update or insert
	UpsertAction = "upsert"
)

var _config *Config

type Config struct {
	Target string `yaml:"target"` // 目标类型，支持redis、mongodb

	Addr     string `yaml:"addr"`
	User     string `yaml:"user"`
	Password string `yaml:"pass"`
	Database string `yaml:"database"`
	Charset  string `yaml:"charset"`

	SlaveID           uint32   `yaml:"slave_id"`
	Flavor            string   `yaml:"flavor"`
	DataDir           string   `yaml:"data_dir"`
	IncludeTableRegex []string `yaml:"include_table_regex"`
	DumpExec          string   `yaml:"mysqldump"`
	SkipMasterData    bool     `yaml:"skip_master_data"`

	Maxprocs int   `yaml:"maxprocs"` // 最大协程数，默认CPU核心数*2
	BulkSize int64 `yaml:"bulk_size"`

	FlushBulkInterval int `yaml:"flush_bulk_interval"`

	SkipNoPkTable bool `yaml:"skip_no_pk_table"`

	LoggerConfig *logs.Config `yaml:"logger"` // 日志配置

	//mysql
	MysqlAddr        string `yaml:"mysql_addr"`
	MysqlUser        string `yaml:"mysql_user"`
	MysqlPassword    string `yaml:"mysql_pass"`
	MysqlDatabase    string `yaml:"mysql_database"`
	isReserveRawData bool   //保留原始数据
	isMQ             bool   //是否消息队列
	//
	EnableExporter bool `yaml:"enable_exporter"` // 启用prometheus exporter，默认false
	ExporterPort   int  `yaml:"exporter_addr"`   // prometheus exporter端口

	EnableWebAdmin bool `yaml:"enable_web_admin"` // 启用Web监控，默认false
	WebAdminPort   int  `yaml:"web_admin_port"`   // web监控端口,默认8060
}

func initConfig(fileName string) error {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return errors.Trace(err)
	}

	var c Config

	if err := yaml.Unmarshal(data, &c); err != nil {
		return errors.Trace(err)
	}

	if err := checkConfig(&c); err != nil {
		return errors.Trace(err)
	}

	switch strings.ToUpper(c.Target) {
	case _targetMysql:

	default:
		return errors.Errorf("unsupported target: %s", c.Target)
	}

	_config = &c

	return nil
}

func checkConfig(c *Config) error {
	if c.Target == "" {
		return errors.Errorf("empty target not allowed")
	}

	if c.Addr == "" {
		return errors.Errorf("empty addr not allowed")
	}

	if c.User == "" {
		return errors.Errorf("empty user not allowed")
	}

	if c.Password == "" {
		return errors.Errorf("empty pass not allowed")
	}

	if c.Charset == "" {
		return errors.Errorf("empty charset not allowed")
	}

	if c.SlaveID == 0 {
		return errors.Errorf("empty slave_id not allowed")
	}

	if c.Flavor == "" {
		c.Flavor = "mysql"
	}

	if c.FlushBulkInterval == 0 {
		c.FlushBulkInterval = _flushBulkInterval
	}

	if c.BulkSize == 0 {
		c.BulkSize = _flushBulkSize
	}

	if c.DataDir == "" {
		c.DataDir = filepath.Join(sys.CurrentDirectory(), _dataDir)
	}

	if err := files.MkdirIfNecessary(c.DataDir); err != nil {
		return err
	}

	if c.LoggerConfig == nil {
		c.LoggerConfig = &logs.Config{
			Store: filepath.Join(c.DataDir, "log"),
		}
	}
	if c.LoggerConfig.Store == "" {
		c.LoggerConfig.Store = filepath.Join(c.DataDir, "log")
	}

	if err := files.MkdirIfNecessary(c.LoggerConfig.Store); err != nil {
		return err
	}

	if c.Maxprocs <= 0 {
		c.Maxprocs = runtime.NumCPU() * 2
	}

	return nil
}

func Cfg() *Config {
	return _config
}

func (c *Config) IsReserveRawData() bool {
	return c.isReserveRawData
}

func (c *Config) InDatabase(dataBase string) bool {

	if strings.Compare(c.Database, dataBase) == 0 {
		return true
	}

	return false
}
