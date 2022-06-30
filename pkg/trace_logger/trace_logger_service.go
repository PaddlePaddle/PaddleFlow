package trace_logger

import (
	go_fmt "fmt"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	log "github.com/sirupsen/logrus"
	"os"
)

var ()

// DeleteFunc delete function for trace log
var DeleteFunc DeleteMethod = func(key string) bool {
	// get run from db
	run, err := models.GetRunByID(log.NewEntry(log.StandardLogger()), key)
	if err != nil {
		// if not found, delete trace log
		return true
	}
	// if run is not finished, keep trace log
	if common.IsRunFinalStatus(run.Status) {
		return true
	}
	return false
}

func Init(config TraceLoggerConfig) error {
	fillDefaultValue(&config)
	return InitTraceLoggerManager(config)
}

func Start(config TraceLoggerConfig) error {
	fillDefaultValue(&config)
	var err error
	// recover local trace log
	err = LoadAll(config.Dir, config.FilePrefix)
	// if error is NotExistErr, omit it
	if err != nil && !os.IsExist(err) {
		errMsg := go_fmt.Errorf("load local trace log failed. error: %w", err)
		return errMsg
	}
	err = nil

	duration, _ := ParseTimeUnit(config.DeleteInterval)
	// enable auto delete and sync for trace log
	if err = AutoDelete(
		duration,
		DeleteFunc,
	); err != nil {
		errMsg := go_fmt.Errorf("enable auto delete for trace log failed: %w", err)
		return errMsg
	}

	duration, _ = ParseTimeUnit(config.DeleteInterval)
	if err = AutoSync(
		duration,
	); err != nil {
		_ = CancelAutoDelete()
		errMsg := go_fmt.Errorf("enable auto sync for trace log failed: %w", err)
		return errMsg
	}
	return nil
}
