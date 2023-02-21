package logagent

import "go-mysql-cdc/util/logs"

type ElsLoggerAgent struct {
}

func NewElsLoggerAgent() *ElsLoggerAgent {
	return &ElsLoggerAgent{}
}

func (s *ElsLoggerAgent) Printf(format string, v ...interface{}) {
	logs.Infof(format, v)
}
