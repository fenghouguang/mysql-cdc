package global

import (
	"fmt"
	sidlog "github.com/siddontang/go-log/log"
	"go-mysql-cdc/util/logs"
	"log"
	"runtime"
	"syscall"
)

var (
	_pid int
)

func Initialize(configPath string) error {
	if err := initConfig(configPath); err != nil {
		return err
	}
	runtime.GOMAXPROCS(_config.Maxprocs)

	// 初始化global logger
	if err := logs.Initialize(_config.LoggerConfig); err != nil {
		return err
	}

	streamHandler, err := sidlog.NewStreamHandler(logs.Writer())
	if err != nil {
		return err
	}
	agent := sidlog.New(streamHandler, sidlog.Ltime|sidlog.Lfile|sidlog.Llevel)
	sidlog.SetDefaultLogger(agent)

	_pid = syscall.Getpid()

	log.Println(fmt.Sprintf("process id: %d", _pid))
	log.Println(fmt.Sprintf("GOMAXPROCS :%d", _config.Maxprocs))
	log.Println(fmt.Sprintf("source  %s(%s)", _config.Flavor, _config.Addr))

	return nil
}
