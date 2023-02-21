package storage

import (
	"github.com/siddontang/go-mysql/mysql"
)

type PositionStorage interface {
	Initialize() error
	Save(pos mysql.Position) error
	Get() (mysql.Position, error)
}

func NewPositionStorage() PositionStorage {

	return &boltPositionStorage{}
}
