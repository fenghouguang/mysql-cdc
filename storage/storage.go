package storage

import (
	"errors"
	"fmt"
	"go-mysql-cdc/global"
	"go-mysql-cdc/util/byteutil"
	"go-mysql-cdc/util/files"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const (
	_boltFilePath = "db"
	_boltFileName = "data.db"
	_boltFileMode = 0600
)

var (
	_positionBucket = []byte("Position")
	_fixPositionId  = byteutil.Uint64ToBytes(uint64(1))

	_bolt        *bbolt.DB
	_zkAddresses []string
)

func Initialize() error {
	if err := initBolt(); err != nil {
		return err
	}

	return nil
}

func initBolt() error {
	blotStorePath := filepath.Join(global.Cfg().DataDir, _boltFilePath)
	if err := files.MkdirIfNecessary(blotStorePath); err != nil {
		return errors.New(fmt.Sprintf("create boltdb store : %s", err.Error()))
	}

	boltFilePath := filepath.Join(blotStorePath, _boltFileName)
	bolt, err := bbolt.Open(boltFilePath, _boltFileMode, bbolt.DefaultOptions)
	if err != nil {
		return errors.New(fmt.Sprintf("open boltdb: %s", err.Error()))
	}

	err = bolt.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(_positionBucket)
		return nil
	})

	_bolt = bolt

	return err
}

func Close() {
	if _bolt != nil {
		_bolt.Close()
	}

}
