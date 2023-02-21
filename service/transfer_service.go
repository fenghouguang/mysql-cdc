package service

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	sc "github.com/siddontang/go-mysql/schema"
	"go.uber.org/atomic"

	"go-mysql-cdc/global"
	"go-mysql-cdc/service/endpoint"
	"go-mysql-cdc/storage"
	"go-mysql-cdc/util/logs"
)

const _transferLoopInterval = 1

type TransferService struct {
	canal        *canal.Canal
	canalCfg     *canal.Config
	canalHandler *handler
	canalEnable  atomic.Bool
	lockOfCanal  sync.Mutex
	firstsStart  atomic.Bool

	wg             sync.WaitGroup
	endpoint       endpoint.Endpoint
	endpointEnable atomic.Bool
	positionDao    storage.PositionStorage
	loopStopSignal chan struct{}
}

func (s *TransferService) initialize() error {
	s.canalCfg = canal.NewDefaultConfig()
	s.canalCfg.Addr = global.Cfg().Addr
	s.canalCfg.User = global.Cfg().User
	s.canalCfg.Password = global.Cfg().Password
	s.canalCfg.Charset = global.Cfg().Charset
	s.canalCfg.Flavor = global.Cfg().Flavor
	s.canalCfg.ServerID = global.Cfg().SlaveID
	s.canalCfg.IncludeTableRegex = global.Cfg().IncludeTableRegex
	s.canalCfg.Dump.ExecutionPath = global.Cfg().DumpExec
	s.canalCfg.Dump.DiscardErr = false
	s.canalCfg.Dump.SkipMasterData = global.Cfg().SkipMasterData

	if err := s.createCanal(); err != nil {
		return errors.Trace(err)
	}

	if err := s.completeRules(); err != nil {
		return errors.Trace(err)
	}

	s.addDumpDatabaseOrTable()

	positionDao := storage.NewPositionStorage()
	if err := positionDao.Initialize(); err != nil {
		return errors.Trace(err)
	}
	s.positionDao = positionDao

	// endpoint
	endpoint := endpoint.NewEndpoint(s.canal)
	if err := endpoint.Connect(); err != nil {
		return errors.Trace(err)
	}
	// 异步，必须要ping下才能确定连接成功
	s.endpoint = endpoint
	s.endpointEnable.Store(true)

	s.firstsStart.Store(true)
	s.startLoop()

	return nil
}

func (s *TransferService) run() error {
	current, err := s.positionDao.Get()
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go func(p mysql.Position) {
		s.canalEnable.Store(true)
		log.Println(fmt.Sprintf("transfer run from position(%s %d)", p.Name, p.Pos))
		if err := s.canal.RunFrom(p); err != nil {
			log.Println(fmt.Sprintf("start transfer : %v", err))
			logs.Errorf("canal : %v", errors.ErrorStack(err))
			if s.canalHandler != nil {
				s.canalHandler.stopListener()
			}
			s.canalEnable.Store(false)
		}

		logs.Info("Canal is Closed")
		s.canalEnable.Store(false)
		s.canal = nil
		s.wg.Done()
	}(current)

	// canal未提供回调，停留一秒，确保RunFrom启动成功
	time.Sleep(time.Second)
	return nil
}

func (s *TransferService) StartUp() {
	s.lockOfCanal.Lock()
	defer s.lockOfCanal.Unlock()

	if s.firstsStart.Load() {
		s.canalHandler = newHandler()
		s.canal.SetEventHandler(s.canalHandler)
		s.canalHandler.startListener()
		s.firstsStart.Store(false)
		s.run()
	} else {
		s.restart()
	}
}

func (s *TransferService) restart() {
	if s.canal != nil {
		s.canal.Close()
		s.wg.Wait()
	}

	s.createCanal()
	s.addDumpDatabaseOrTable()
	s.canalHandler = newHandler()
	s.canal.SetEventHandler(s.canalHandler)
	s.canalHandler.startListener()
	s.run()
}

func (s *TransferService) stopDump() {
	s.lockOfCanal.Lock()
	defer s.lockOfCanal.Unlock()

	if s.canal == nil {
		return
	}

	if !s.canalEnable.Load() {
		return
	}

	if s.canalHandler != nil {
		s.canalHandler.stopListener()
		s.canalHandler = nil
	}

	s.canal.Close()
	s.wg.Wait()

	log.Println("dumper stopped")
}

func (s *TransferService) Close() {
	s.stopDump()
	s.loopStopSignal <- struct{}{}
}

func (s *TransferService) Position() (mysql.Position, error) {
	return s.positionDao.Get()
}

func (s *TransferService) createCanal() error {
	var err error
	s.canal, err = canal.NewCanal(s.canalCfg)
	return errors.Trace(err)
}

func (s *TransferService) completeRules() error {
	global.TableInfoReset()
	sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE table_schema = "%s";`, global.Cfg().Database)
	res, err := s.canal.Execute(sql)
	if err != nil {
		return errors.Trace(err)
	}
	for i := 0; i < res.Resultset.RowNumber(); i++ {
		tableName, _ := res.GetString(i, 0)
		//tableInfo := &global.TableInfo{Schema: global.Cfg().Database, Table: tableName}
		//newRule := &global.Rule{}
		//newRule.Schema = global.Cfg().Database
		//newRule.Table = tableName
		tableMata, err := s.canal.GetTable(global.Cfg().Database, tableName)
		if err != nil {
			if err == canal.ErrExcludedTable {
				continue
			} else {
				return errors.Trace(err)
			}
		}
		ruleKey := global.TableInfoKey(global.Cfg().Database, tableName)
		global.AddTableInfo(ruleKey, &global.TableInfo{Table: tableMata})
	}
	return nil
}

func (s *TransferService) addDumpDatabaseOrTable() {
	var schema string
	schemas := make(map[string]int)
	tables := make([]string, 0, global.RuleInsTotal())
	for _, rule := range global.RuleInsList() {
		schema = rule.Schema
		schemas[rule.Schema] = 1
		tables = append(tables, rule.Name)
	}
	if len(schemas) == 1 {
		s.canal.AddDumpTables(schema, tables...)
	} else {
		keys := make([]string, 0, len(schemas))
		for key := range schemas {
			keys = append(keys, key)
		}
		s.canal.AddDumpDatabases(keys...)
	}
}

func (s *TransferService) updateTableInfo(schema, table string) error {
	ti, ok := global.TableInfoIns(global.TableInfoKey(schema, table))
	tableInfo, err := s.canal.GetTable(schema, table)
	if err != nil {
		if err == sc.ErrTableNotExist || err == canal.ErrExcludedTable {
			global.DeleteTableInfo(global.TableInfoKey(schema, table))
			return nil
		} else {
			return errors.Trace(err)
		}
	}
	if ok {
		if len(tableInfo.PKColumns) == 0 {
			if !global.Cfg().SkipNoPkTable {
				return errors.Errorf("%s.%s must have a PK for a column", ti.Schema, ti.Table)
			}
		}

		ti.Table = tableInfo
	} else {
		global.AddTableInfo(global.TableInfoKey(schema, table), &global.TableInfo{
			Table: tableInfo,
		})
	}
	return nil
}

func (s *TransferService) startLoop() {
	go func() {
		ticker := time.NewTicker(_transferLoopInterval * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !s.endpointEnable.Load() {
					err := s.endpoint.Ping()
					if err != nil {
						log.Println("destination not available,see the log file for details")
						logs.Error(err.Error())
					} else {
						s.endpointEnable.Store(true)
						s.StartUp()
					}
				}
			case <-s.loopStopSignal:
				return
			}
		}
	}()
}
