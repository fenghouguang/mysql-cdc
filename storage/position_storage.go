package storage

import (
	"github.com/go-mysql-org/go-mysql/mysql"
)

type PositionStorage interface {
	Initialize() error
	Save(pos mysql.Position) error
	Get() (mysql.Position, error)
}

func NewPositionStorage() PositionStorage {

	return &boltPositionStorage{}
}
