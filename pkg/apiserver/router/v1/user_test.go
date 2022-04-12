package v1

import (
	"testing"

	"github.com/go-chi/chi"
	"github.com/stretchr/testify/assert"

	"paddleflow/pkg/apiserver/router/util"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/database/dbinit"
	"paddleflow/pkg/common/logger"
)

func prepareDBAndAPIForUser(t *testing.T, userName string) (*chi.Mux, string) {
	chiRouter := NewApiTest()
	baseUrl := util.PaddleflowRouterPrefix + util.PaddleflowRouterVersionV1

	config.GlobalServerConfig = &config.ServerConfig{
		ApiServer: config.ApiServerConfig{
			TokenExpirationHour: -1,
		},
	}
	dbinit.InitMockDB()
	if userName == "" {
		userName = MockRootUser
	}
	ctx := &logger.RequestContext{UserName: userName}

	token, err := CreateTestUser(ctx, userName, MockPassword)
	assert.Nil(t, err)
	setToken(token)

	return chiRouter, baseUrl
}
