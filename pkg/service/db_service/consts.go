/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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

package db_service

const (
	QueryEqualWithParam = " (%s = ?) "
	QueryLess           = " (%s <= %s) "
	QueryGreater        = " (%s >= %d) "
	QueryLikeWithParam  = " (%s LIKE ?) "
	QueryInWithParam    = " (%s IN (?)) "
	QueryNotInWithParam = " (%s NOT IN (?)) "
	QueryIsNull         = " (%s IS NULL) "
	QueryAnd            = " AND "
	QueryOr             = " OR "

	Type          = "type"
	ServerAddress = "server_address"
	State         = "state"
	ID            = "id"
	FsID          = "fs_id"
	FsCacheID     = "cache_id"
	FsPath        = "fs_path"
	Address       = "address"
	UserName      = "user_name"
	FsName        = "name"
	GrantFsType   = "fs"

	MAXListFileSystems = 1000

	FsPrefix  = "fs-"
	FsNameNum = 8

	CreatedAt = "created_at"
	UpdatedAt = "updated_at"

	ASC  = "asc"
	DESC = "desc"
)
