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

package location_awareness

import (
	"math/rand"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
)

func ReportCacheLoop(cacheReport api.CacheReportParams, podCachePath string, httpClient *core.PaddleFlowClient) error {
	var err, errStat error
	var usageStat *disk.UsageStat
	for {
		usageStat, errStat = disk.Usage(podCachePath)
		if errStat != nil {
			log.Errorf("disk stat path[%s] and err[%v]", podCachePath, errStat)
			time.Sleep(1 * time.Second)
			continue
		}
		cacheReport.UsedSize = int(usageStat.Used / 1024)
		err = api.CacheReportRequest(cacheReport, httpClient)
		if err != nil {
			log.Errorf("cache report failed with params[%+v] and err[%v]", cacheReport, err)
		}
		select {
		case <-time.After(time.Duration(15+rand.Intn(10)) * time.Second):
		}
	}
	return nil
}
