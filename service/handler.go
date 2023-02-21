package service

import (
	"log"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"go-mysql-cdc/global"
	"go-mysql-cdc/model"
	"go-mysql-cdc/util/logs"
)

type handler struct {
	queue chan interface{}
	stop  chan struct{}
}

func newHandler() *handler {
	return &handler{
		queue: make(chan interface{}, 4096),
		stop:  make(chan struct{}, 1),
	}
}

func (s *handler) OnRotate(e *replication.RotateEvent) error {
	s.queue <- model.PosRequest{
		Name:  string(e.NextLogName),
		Pos:   uint32(e.Position),
		Force: true,
	}
	return nil
}

func (s *handler) OnTableChanged(schema, table string) error {
	if !global.Cfg().InDatabase(schema) {
		return nil
	}
	err := _transferService.updateTableInfo(schema, table)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *handler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	s.queue <- model.PosRequest{
		Name:       nextPos.Name,
		Pos:        nextPos.Pos,
		Force:      true,
		QueryEvent: queryEvent,
	}
	if queryEvent != nil && global.Cfg().InDatabase(string(queryEvent.Schema)) {
		//logs.Info(fmt.Sprintf("Query: %s\n", queryEvent.Query))
		//err := _transferService.completeRules()
		//if err != nil {
		//	return errors.Trace(err)
		//}
		s.queue <- model.DDLRequest{
			SlaveProxyID:  queryEvent.SlaveProxyID,
			ExecutionTime: queryEvent.ExecutionTime,
			ErrorCode:     queryEvent.ErrorCode,
			StatusVars:    string(queryEvent.StatusVars),
			Schema:        string(queryEvent.Schema),
			Query:         strings.Replace(string(queryEvent.Query), string(queryEvent.Schema), global.Cfg().MysqlDatabase, -1),
		}
	}
	return nil
}

func (s *handler) OnXID(nextPos mysql.Position) error {
	s.queue <- model.PosRequest{
		Name:  nextPos.Name,
		Pos:   nextPos.Pos,
		Force: false,
	}
	return nil
}

func (s *handler) OnRow(e *canal.RowsEvent) error {
	if !global.Cfg().InDatabase(e.Table.Schema) {
		return nil
	}
	ruleKey := global.TableInfoKey(e.Table.Schema, e.Table.Name)
	if !global.TableInsExist(ruleKey) {
		err := _transferService.updateTableInfo(e.Table.Schema, e.Table.Name)
		if err != nil {
			return errors.Trace(err)
		}
	}
	var requests []*model.RowRequest
	if e.Action != canal.UpdateAction {
		// 定长分配
		requests = make([]*model.RowRequest, 0, len(e.Rows))
	}

	if e.Action == canal.UpdateAction {
		for i := 0; i < len(e.Rows); i++ {
			if (i+1)%2 == 0 {
				v := new(model.RowRequest)
				v.RuleKey = ruleKey
				v.Action = e.Action
				v.Timestamp = e.Header.Timestamp
				if global.Cfg().IsReserveRawData() {
					v.Old = e.Rows[i-1]
				}
				v.Row = e.Rows[i]
				requests = append(requests, v)
			}
		}
	} else {
		for _, row := range e.Rows {
			v := new(model.RowRequest)
			v.RuleKey = ruleKey
			v.Action = e.Action
			v.Timestamp = e.Header.Timestamp
			v.Row = row
			requests = append(requests, v)
		}
	}
	s.queue <- requests

	return nil
}

func (s *handler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (s *handler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (s *handler) String() string {
	return "TransferHandler"
}

func (s *handler) startListener() {
	go func() {
		interval := time.Duration(global.Cfg().FlushBulkInterval)
		bulkSize := global.Cfg().BulkSize
		ticker := time.NewTicker(time.Millisecond * interval)
		defer ticker.Stop()

		lastSavedTime := time.Now()
		requests := make([]*model.RowRequest, 0, bulkSize)
		var current mysql.Position
		from, _ := _transferService.positionDao.Get()
		for {
			needFlush := false
			needSavePos := false
			select {
			case v := <-s.queue:
				switch v := v.(type) {
				case model.PosRequest:
					now := time.Now()
					if v.Force || now.Sub(lastSavedTime) > 3*time.Second {
						lastSavedTime = now
						needFlush = true
						needSavePos = true
						current = mysql.Position{
							Name: v.Name,
							Pos:  v.Pos,
						}
					}
				case model.DDLRequest:
					rs := make([]*model.RowRequest, 0)
					r := &model.RowRequest{Action: "DDL", Sql: v.Query}
					rs = append(rs, r)
					requests = append(requests, rs...)
					needFlush = int64(len(requests)) >= global.Cfg().BulkSize
				case []*model.RowRequest:
					requests = append(requests, v...)
					needFlush = int64(len(requests)) >= global.Cfg().BulkSize
				}
			case <-ticker.C:
				needFlush = true
			case <-s.stop:
				return
			}

			if needFlush && len(requests) > 0 && _transferService.endpointEnable.Load() {
				err := _transferService.endpoint.Consume(from, requests)
				if err != nil {
					_transferService.endpointEnable.Store(false)
					logs.Error(err.Error())
					go _transferService.stopDump()
				}
				requests = requests[0:0]
			}
			if needSavePos && _transferService.endpointEnable.Load() {
				logs.Infof("save position %s %d", current.Name, current.Pos)
				if err := _transferService.positionDao.Save(current); err != nil {
					logs.Errorf("save sync position %s err %v, close sync", current, err)
					_transferService.Close()
					return
				}
				from = current
			}
		}
	}()
}

func (s *handler) stopListener() {
	log.Println("transfer stop")
	s.stop <- struct{}{}
}
