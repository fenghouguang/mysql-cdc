package global

type Status struct {
	BinlogFile  string
	BinlogPos   uint32
	InsertCount int64
	UpdateCount int64
	DeleteCount int64
	DDLCount    int64
}

var CanalStatus = Status{}
