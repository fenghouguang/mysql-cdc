package endpoint

import (
	"fmt"
	"github.com/go-redis/redis"
	"strings"
	"sync"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	gormsql "gorm.io/driver/mysql"
	"gorm.io/gorm"

	"go-mysql-cdc/global"
	"go-mysql-cdc/model"
	"go-mysql-cdc/util/logs"
)

type MysqlEndpoint struct {
	retryLock sync.Mutex
	db        *gorm.DB
}

func newMysqlEndpoint() *MysqlEndpoint {
	cfg := global.Cfg()
	r := &MysqlEndpoint{}
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", cfg.MysqlUser,
		cfg.MysqlPassword, cfg.MysqlAddr, cfg.MysqlDatabase)
	r.db, _ = gorm.Open(gormsql.Open(dsn), &gorm.Config{})
	return r
}

func (s *MysqlEndpoint) Connect() error {
	return nil
}

func (s *MysqlEndpoint) Ping() error {
	//var err error
	//if s.isCluster {
	//	_, err = s.cluster.Ping().Result()
	//} else {
	//	_, err = s.client.Ping().Result()
	//}
	return nil
}

func (s *MysqlEndpoint) pipe() redis.Pipeliner {
	//var pipe redis.Pipeliner
	//if s.isCluster {
	//	pipe = s.cluster.Pipeline()
	//} else {
	//	pipe = s.client.Pipeline()
	//}
	//return pipe
	return nil
}

func (s *MysqlEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	//pipe := s.pipe()
	for _, row := range rows {
		if row.Action == "DDL" {
			s.db.Exec(row.Sql)
			continue
		}
		rule, _ := global.TableInfoIns(row.RuleKey)
		if rule == nil {
			logs.Error("TableInfo is not found")
			continue
		}
		_ = s.ruleRespond(row, rule)

	}

	//_, err := pipe.Exec()
	//if err != nil {
	//	return err
	//}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil
}

func (s *MysqlEndpoint) Stock(rows []*model.RowRequest) int64 {
	//pipe := s.pipe()
	//for _, row := range rows {
	//	rule, _ := global.TableInfoIns(row.RuleKey)
	//	if rule.TableColumnSize != len(row.Row) {
	//		logs.Warnf("%s schema mismatching", row.RuleKey)
	//		continue
	//	}
	//}
	//
	//var counter int64
	//res, err := pipe.Exec()
	//if err != nil {
	//	logs.Error(err.Error())
	//}
	//
	//for _, re := range res {
	//	if re.Err() == nil {
	//		counter++
	//	}
	//}
	//
	//return counter
	return 0
}

func (s *MysqlEndpoint) ruleRespond(row *model.RowRequest, rule *global.TableInfo) *model.MysqlRespond {
	resp := new(model.MysqlRespond)
	kvm := rowMap(row, rule, false)
	ruleKey := strings.Split(row.RuleKey, ":")
	if len(ruleKey) > 1 {
		table := ruleKey[1]
		if row.Action == canal.InsertAction {
			s.db.Table(table).Create(kvm)
		} else if row.Action == canal.UpdateAction {
			if s.db.Table(table).Where("id=?", kvm["id"]).Omit("id").Updates(kvm).RowsAffected == 0 {
				s.db.Table(table).Create(kvm)
			}
		} else if row.Action == canal.DeleteAction {
			s.db.Table(table).Delete(&struct{}{}, "id=?", kvm["id"])
		}
	}
	return resp
}

func (s *MysqlEndpoint) preparePipe(resp *model.RedisRespond, pipe redis.Cmdable) {

}

func (s *MysqlEndpoint) Close() {
}
