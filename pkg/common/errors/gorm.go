package errors

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
)

const (
	// mysql errr number reference: https://mariadb.com/kb/en/mariadb-error-codes/
	ErrNoDuplicateEntry = 1062
	ErrNoKeyNotFound    = 1032

	ErrorUnknown         = "UnknownError"      // 数据库错误
	ErrorKeyIsDuplicated = "DBKeyIsDuplicated" // 数据库key重复
	ErrorRecordNotFound  = "RecordNotFound"    //记录不存在
)

type GormErr struct {
	Number  int    `json:"Number"`
	Message string `json:"Message"`
}

func GetErrorCode(err error) string {
	byteErr, _ := json.Marshal(err)
	var gormErr GormErr
	if err := json.Unmarshal(byteErr, &gormErr); err != nil {
		return ErrorUnknown
	}
	switch gormErr.Number {
	case ErrNoDuplicateEntry:
		logrus.Errorf("database key is duplicated. err:%s", gormErr.Message)
		return ErrorKeyIsDuplicated
	case ErrNoKeyNotFound:
		logrus.Errorf("database record not found. err:%s", gormErr.Message)
		return ErrorRecordNotFound
	}
	return ErrorUnknown
}
