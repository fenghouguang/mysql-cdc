package model

import (
	"github.com/siddontang/go-mysql/replication"
	"sync"
)

var RowRequestPool = sync.Pool{
	New: func() interface{} {
		return new(RowRequest)
	},
}

type RowRequest struct {
	RuleKey   string
	Action    string
	Timestamp uint32
	Old       []interface{}
	Row       []interface{}
	Sql       string
}

type PosRequest struct {
	Name       string
	Pos        uint32
	Force      bool
	QueryEvent *replication.QueryEvent
}
type DDLRequest struct {
	SlaveProxyID  uint32
	ExecutionTime uint32
	ErrorCode     uint16
	StatusVars    string
	Schema        string
	Query         string
}

func BuildRowRequest() *RowRequest {
	return RowRequestPool.Get().(*RowRequest)
}

func ReleaseRowRequest(t *RowRequest) {
	RowRequestPool.Put(t)
}
