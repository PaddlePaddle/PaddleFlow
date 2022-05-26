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

package v1

import (
	"net/http"

	"github.com/go-chi/chi"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/user"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/middleware"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
)

type UserRouter struct{}

func (ur *UserRouter) Name() string {
	return "User"
}

func (ur *UserRouter) AddRouter(r chi.Router) {
	log.Info("add user router")
	r.Post("/login", ur.login)
	r.Post("/user", ur.createUser)
	r.Delete("/user/{username}", ur.deleteUser)
	r.Put("/user/{username}", ur.updateUser)
	r.Get("/user", ur.listUser)

}

// login
// @Summary 用户登录
// @Description 用户登录
// @Id login
// @tags User
// @Accept  json
// @Produce json
// @Param request body user.LoginInfo true "用户登录请求"
// @Success 200 {object} user.LoginResponse "创建用户响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /user [POST]
func (ur *UserRouter) login(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var req user.LoginInfo
	err := common.BindJSON(r, &req)
	if err != nil {
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		ctx.Logging().Errorf("user login bindjson failed. error:%s", err.Error())
		return
	}
	log.Debugf("login req:[%s]", config.PrettyFormat(req))
	u, err := user.Login(&ctx, req.UserName, req.Password, false)
	if err != nil || u.Name == "" {
		ctx.Logging().Errorf(
			"user login failed. username:%v error:%s", req.UserName, err.Error())
		common.RenderErr(w, ctx.RequestID, ctx.ErrorCode)
		return
	}
	token, err := middleware.GenerateToken(u.Name, u.Password)
	if err != nil {
		ctx.Logging().Errorf(
			"generate token failed. username:%v error:%s", req.UserName, err.Error())
		common.RenderErr(w, ctx.RequestID,
			common.AuthFailed)
		return
	}
	loginResp := user.LoginResponse{Authorization: token}
	common.Render(w, http.StatusOK, loginResp)
}

// createUser
// @Summary 创建用户
// @Description 创建用户
// @Id createUser
// @tags User
// @Accept  json
// @Produce json
// @Param request body user.LoginInfo true "创建用户请求"
// @Success 200 {object} user.CreateUserResponse "创建用户响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /user [POST]
func (ur *UserRouter) createUser(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var userInfo user.LoginInfo
	err := common.BindJSON(r, &userInfo)
	if err != nil {
		ctx.Logging().Errorf("create user bind json failed. error:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	response, err := user.CreateUser(&ctx, userInfo.UserName, userInfo.Password)
	if err != nil {
		ctx.Logging().Errorf(
			"Create user failed. error:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, ctx.ErrorCode)
		return
	}
	common.Render(w, http.StatusOK, response)
}

// deleteUser
// @Summary 删除用户
// @Description 删除用户
// @Id deleteUser
// @tags User
// @Accept  json
// @Produce json
// @Param username path string true "用户名称"
// @Success 200 {string} string "成功删除用户的响应码"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /user/{username} [DELETE]
func (ur *UserRouter) deleteUser(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	userName := chi.URLParam(r, util.QueryKeyUserName)
	err := user.DeleteUser(&ctx, userName)
	if err != nil {
		ctx.Logging().Errorf("delete user failed.  error:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, ctx.ErrorCode)
		return
	}
	common.RenderStatus(w, http.StatusOK)
}

// updateUser
// @Summary 更新用户
// @Description 更新用户
// @Id updateUser
// @tags User
// @Accept  json
// @Produce json
// @Param username path string true "用户名称"
// @Param request body user.UpdateUserArgs true "用户更新信息"
// @Success 200 {string} string "成功更新用户的响应码"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /user/{username} [PUT]
func (ur *UserRouter) updateUser(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	userName := chi.URLParam(r, util.QueryKeyUserName)
	var pd user.UpdateUserArgs
	err := common.BindJSON(r, &pd)
	if err != nil {
		ctx.Logging().Errorf("update user bind json failed. error:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	err = user.UpdateUser(&ctx, userName, pd.Password)
	if err != nil {
		ctx.Logging().Errorf("update user's password failed. userName:%s, error:%s", userName, err.Error())
		common.RenderErr(w, ctx.RequestID, ctx.ErrorCode)
		return
	}
	common.RenderStatus(w, http.StatusOK)
}

// listUser
// @Summary 获取用户列表
// @Description 获取用户列表
// @Id listUser
// @tags User
// @Accept  json
// @Produce json
// @Param user query string false "用户名称过滤"
// @Param maxKeys query int false "每页包含的最大数量，缺省值为50"
// @Param marker query string false "批量获取列表的查询的起始位置，是一个由系统生成的字符串"
// @Success 200 {object} user.ListUserResponse "获取用户列表的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /user [GET]
func (ur *UserRouter) listUser(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	if username := r.URL.Query().Get(util.QueryKeyUser); username != "" {
		ur.getUser(w, r, username)
		return
	}
	marker := r.URL.Query().Get(util.QueryKeyMarker)
	maxKeys, err := util.GetQueryMaxKeys(&ctx, r)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}

	ctx.Logging().Debugf(
		"list users marker:[%s] maxKeys:[%d] ", marker, maxKeys)
	listUserResponse, err := user.ListUser(&ctx, marker, maxKeys)
	if err != nil {
		common.RenderErr(w, ctx.RequestID, ctx.ErrorCode)
		return
	}
	common.Render(w, http.StatusOK, listUserResponse)
}

func (ur *UserRouter) getUser(w http.ResponseWriter, r *http.Request, username string) {
	ctx := common.GetRequestContext(r)
	response, err := user.GetUserByName(&ctx, username)
	if err != nil {
		ctx.Logging().Errorf("get user by name failed. username:%s, error:%s", username, err.Error())
		common.RenderErr(w, ctx.RequestID, err.Error())
		return
	}
	common.Render(w, http.StatusOK, response)
}
