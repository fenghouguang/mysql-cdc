package collections

import "go-mysql-cdc/util/stringutil"

func Contain(array []string, v interface{}) bool {
	vvv := stringutil.ToString(v)
	for _, vv := range array {
		if vv == vvv {
			return true
		}
	}
	return false
}
