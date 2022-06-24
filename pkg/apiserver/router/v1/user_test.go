package v1

import (
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
	"testing"

	"github.com/go-chi/chi"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

func prepareDBAndAPIForUser(t *testing.T, userName string) (*chi.Mux, string) {
	chiRouter := NewApiTest()
	baseUrl := util.PaddleflowRouterPrefix + util.PaddleflowRouterVersionV1

	config.GlobalServerConfig = &config.ServerConfig{
		ApiServer: config.ApiServerConfig{
			TokenExpirationHour: -1,
		},
	}
	driver.InitMockDB()
	if userName == "" {
		userName = MockRootUser
	}
	ctx := &logger.RequestContext{UserName: userName}

	token, err := CreateTestUser(ctx, userName, MockPassword)
	assert.Nil(t, err)
	setToken(token)

	return chiRouter, baseUrl
}
