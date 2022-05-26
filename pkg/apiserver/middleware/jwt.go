/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package middleware

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/user"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

type JWT struct {
	Sigkey []byte
}

const SECERTKEY = "its my precious"
const UserNameParam = "userName"

var jwtObj *JWT

func init() {
	jwtObj = &JWT{
		Sigkey: []byte(SECERTKEY),
	}
}

type PaddleFlowClaims struct {
	UserName string `json:"username"`
	Password string `json:"password"`
	jwtgo.StandardClaims
}

func (j *JWT) CreateToken(claim PaddleFlowClaims) (string, error) {
	token := jwtgo.NewWithClaims(jwtgo.SigningMethodHS256, claim)
	return token.SignedString(j.Sigkey)
}

func (j *JWT) ParseToken(tokenString string) (*PaddleFlowClaims, error) {
	token, err := jwtgo.ParseWithClaims(tokenString,
		&PaddleFlowClaims{},
		func(token *jwtgo.Token) (interface{}, error) {
			return j.Sigkey, nil
		})
	if err != nil {
		if ve, ok := err.(*jwtgo.ValidationError); ok {
			if ve.Errors&jwtgo.ValidationErrorMalformed != 0 || ve.Errors&jwtgo.ValidationErrorExpired != 0 ||
				ve.Errors&jwtgo.ValidationErrorNotValidYet != 0 {
				return nil, errors.New(common.AuthInvalidToken)
			} else {
				return nil, errors.New(common.AuthFailed)
			}
		}
	}
	if claims, ok := token.Claims.(*PaddleFlowClaims); ok && token.Valid {
		log.Debugf("ParseToken succeed. token:[%s] userName:[%s]", tokenString, claims.UserName)
		return claims, nil
	}
	return nil, errors.New(common.AuthInvalidToken)
}

func GenerateToken(userName string, password string) (string, error) {
	log.Debugf("GenerateToken userName:[%s] password:[%s]", userName, password)
	claim := &PaddleFlowClaims{
		UserName: userName,
		Password: password,
	}
	if config.GlobalServerConfig.ApiServer.TokenExpirationHour != -1 {
		expire := time.Now().Add(time.Duration(config.GlobalServerConfig.ApiServer.TokenExpirationHour) * time.Hour)
		claim.ExpiresAt = expire.Unix()
	}
	claim.NotBefore = int64(time.Now().Unix()) - 1000
	claim.Issuer = "paddleflow"
	token, err := jwtObj.CreateToken(*claim)
	if err != nil {
		return "", errors.New(common.InternalError)
	}
	return token, nil
}

func BaseAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "login") || strings.HasSuffix(req.URL.Path, "login/") {
			next.ServeHTTP(res, req)
			return
		}
		// process requestID and userName
		requestID := req.Header.Get(common.HeaderKeyRequestID)
		userName := req.Header.Get(common.HeaderKeyUserName)
		log.Debugf("GetRequestContext requestID:[%s] userName:[%s]", requestID, userName)
		ctx := logger.RequestContext{RequestID: requestID, UserName: userName}
		ctx.Logging().Debugf("BaseAuth begin. request:%v", req)
		token := req.Header.Get(common.HeaderKeyAuthorization)
		if token == "" {
			ctx.Logging().Errorf("BaseAuth without token. request:%v", req)
			common.RenderErr(res, requestID, common.AuthWithoutToken)
			return
		}
		claims, err := jwtObj.ParseToken(token)
		if err != nil {
			ctx.Logging().Errorf("BaseAuth invalid token[%s]", token)
			common.RenderErr(res, requestID, common.AuthInvalidToken)
			return
		}
		if !checkUserPermission(req, claims.UserName) {
			ctx.Logging().Errorf(
				"BaseAuth user verify error. UserName:[%s] has no permission to operate other user", claims.UserName)
			common.RenderErr(res, requestID, common.AuthIllegalUser)
			return
		}
		_, err = user.Login(&ctx, claims.UserName, claims.Password, true)
		if err != nil {
			ctx.Logging().Errorf(
				"BaseAuth user verify error. UserName:[%s]", claims.UserName)
			common.RenderErr(res, requestID, ctx.ErrorCode)
			return
		}

		ctx.Logging().Debugf("BaseAuth add user-name[%s]", claims.UserName)
		req.Header.Set(common.HeaderKeyUserName, claims.UserName)
		next.ServeHTTP(res, req)
	})
}

type request struct {
	UserName string `json:"userName"`
}

// checkUserPermission 检查用户是否有操作其他用户的权限
func checkUserPermission(r *http.Request, userName string) bool {
	requestUserName := ""
	req := &request{}
	if r.Method == http.MethodPost || r.Method == http.MethodPut {
		bodyBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Errorf("get body err: %v", err)
			return false
		}
		// 保证body下一次能够读取
		r.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
		log.Debugf("post body %s", string(bodyBytes))
		if len(bodyBytes) == 0 {
			return true
		}
		err = json.Unmarshal(bodyBytes, req)
		if err != nil {
			log.Errorf("unmarshal body err: %v", err)
			return true
		}
		requestUserName = req.UserName
	} else {
		values := r.URL.Query()
		requestUserName = values.Get(UserNameParam)
	}
	log.Debugf("requestUserName: %s", requestUserName)
	if requestUserName != "" && requestUserName != userName && userName != common.UserRoot {
		return false
	}
	return true
}
