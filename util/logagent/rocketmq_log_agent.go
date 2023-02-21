package logagent

import (
	"fmt"
	"go.uber.org/zap/zapcore"

	"go-mysql-cdc/util/logs"
)

type RocketmqLoggerAgent struct {
}

func NewRocketmqLoggerAgent() *RocketmqLoggerAgent {
	return &RocketmqLoggerAgent{}
}

func (s *RocketmqLoggerAgent) Debug(msg string, fields map[string]interface{}) {
	zapFields := make([]zapcore.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zapcore.Field{
			Key:    k,
			Type:   zapcore.StringType,
			String: fmt.Sprintf("%v", v),
		})
	}
	logs.Debug(msg, zapFields...)
}

func (s *RocketmqLoggerAgent) Info(msg string, fields map[string]interface{}) {
	zapFields := make([]zapcore.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zapcore.Field{
			Key:    k,
			Type:   zapcore.StringType,
			String: fmt.Sprintf("%v", v),
		})
	}
	logs.Info(msg, zapFields...)
}

func (s *RocketmqLoggerAgent) Warning(msg string, fields map[string]interface{}) {
	zapFields := make([]zapcore.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zapcore.Field{
			Key:    k,
			Type:   zapcore.StringType,
			String: fmt.Sprintf("%v", v),
		})
	}
	logs.Warn(msg, zapFields...)
}

func (s *RocketmqLoggerAgent) Error(msg string, fields map[string]interface{}) {
	zapFields := make([]zapcore.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zapcore.Field{
			Key:    k,
			Type:   zapcore.StringType,
			String: fmt.Sprintf("%v", v),
		})
	}
	logs.Error(msg, zapFields...)
}

func (s *RocketmqLoggerAgent) Fatal(msg string, fields map[string]interface{}) {
	s.Error(msg, fields)
}
