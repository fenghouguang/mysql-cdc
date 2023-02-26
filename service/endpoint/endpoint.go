package endpoint

import (
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	jsoniter "github.com/json-iterator/go"
	"strconv"
	"strings"

	"go-mysql-cdc/global"
	"go-mysql-cdc/model"
	"go-mysql-cdc/util/logs"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const defaultDateFormatter = "2006-01-02"

type Endpoint interface {
	Connect() error
	Ping() error
	Consume(mysql.Position, []*model.RowRequest) error
	Stock([]*model.RowRequest) int64
	Close()
}

func NewEndpoint(ds *canal.Canal) Endpoint {

	return newMysqlEndpoint()

}

func convertColumnData(value interface{}, col *schema.TableColumn) interface{} {
	if value == nil {
		return nil
	}

	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				logs.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}
			return col.EnumValues[eNum]
		case string:
			return value
		case []byte:
			return string(value)
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			if value == "\x01" {
				return int64(1)
			}
			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		var vv string
		switch v := value.(type) {
		case string:
			vv = v
		case []byte:
			vv = string(v)
		}
		return vv
	case schema.TYPE_DATE:
		var vv string
		switch v := value.(type) {
		case string:
			vv = v
		case []byte:
			vv = string(v)
		}
		return vv
	case schema.TYPE_NUMBER:
		switch v := value.(type) {
		case string:
			vv, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				logs.Error(err.Error())
				return nil
			}
			return vv
		case []byte:
			str := string(v)
			vv, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				logs.Error(err.Error())
				return nil
			}
			return vv
		}
	case schema.TYPE_DECIMAL, schema.TYPE_FLOAT:
		switch v := value.(type) {
		case string:
			vv, err := strconv.ParseFloat(v, 64)
			if err != nil {
				logs.Error(err.Error())
				return nil
			}
			return vv
		case []byte:
			str := string(v)
			vv, err := strconv.ParseFloat(str, 64)
			if err != nil {
				logs.Error(err.Error())
				return nil
			}
			return vv
		}
	}

	return value
}

func rowMap(req *model.RowRequest, rule *global.TableInfo, primitive bool) map[string]interface{} {
	kv := make(map[string]interface{}, len(rule.Columns))
	for i, padding := range rule.Columns {
		kv[padding.Name] = convertColumnData(req.Row[i], &padding)
	}
	return kv
}
