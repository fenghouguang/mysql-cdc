package global

import (
	"github.com/siddontang/go-mysql/schema"
	"strings"
	"sync"
)

var (
	_tableInfoMap     = make(map[string]*TableInfo)
	_lockOfRuleInsMap sync.RWMutex
)

type TableInfo struct {
	*schema.Table
}

//type Rule struct {
//	Schema               string `yaml:"schema"`
//	Table                string `yaml:"table"`
//	ColumnMappingConfigs string `yaml:"column_mappings"` // 列名称映射
//
//	// --------------- no config ----------------
//	TableInfo             *schema.Table
//	TableColumnSize       int
//	IsCompositeKey        bool //是否联合主键
//	DefaultColumnValueMap map[string]string
//	PaddingMap            map[string]*model.Padding
//	ValueTmpl             *template.Template
//}

func TableInfoKey(schema string, table string) string {
	return strings.ToLower(schema + ":" + table)
}

func TableInfoReset() {
	_lockOfRuleInsMap.Lock()
	defer _lockOfRuleInsMap.Unlock()
	_tableInfoMap = nil
	_tableInfoMap = make(map[string]*TableInfo)
}
func AddTableInfo(ruleKey string, r *TableInfo) {
	_lockOfRuleInsMap.Lock()
	defer _lockOfRuleInsMap.Unlock()

	_tableInfoMap[ruleKey] = r
}
func DeleteTableInfo(ruleKey string) {
	_lockOfRuleInsMap.Lock()
	defer _lockOfRuleInsMap.Unlock()
	delete(_tableInfoMap, ruleKey)
}

func TableInfoIns(ruleKey string) (*TableInfo, bool) {
	_lockOfRuleInsMap.RLock()
	defer _lockOfRuleInsMap.RUnlock()

	r, ok := _tableInfoMap[ruleKey]
	return r, ok
}

func TableInsExist(ruleKey string) bool {
	_lockOfRuleInsMap.RLock()
	defer _lockOfRuleInsMap.RUnlock()

	_, ok := _tableInfoMap[ruleKey]

	return ok
}

func RuleInsTotal() int {
	_lockOfRuleInsMap.RLock()
	defer _lockOfRuleInsMap.RUnlock()

	return len(_tableInfoMap)
}

func RuleInsList() []*TableInfo {
	_lockOfRuleInsMap.RLock()
	defer _lockOfRuleInsMap.RUnlock()

	list := make([]*TableInfo, 0, len(_tableInfoMap))
	for _, rule := range _tableInfoMap {
		list = append(list, rule)
	}

	return list
}
