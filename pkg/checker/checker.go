/*
Copyright © 2020 Marvin

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
package checker

import (
	"time"

	"github.com/WentaoJin/tichecker/db"

	"github.com/WentaoJin/tichecker/pkg/config"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// 程序运行
func Run(cfg *config.Cfg) error {
	log.Info("Welcome to tichecker", zap.String("config", cfg.String()))
	startTime := time.Now()
	log.Info("tichecker online check all table start")
	// 环境准备
	if err := prepareCheckerENV(cfg); err != nil {
		return err
	}

	// online check
	if err := startOnlineChecker(cfg); err != nil {
		return err
	}

	// 判断上下游是否存在数据不一致
	if err := db.GetMySQLTableFailureRecord(); err != nil {
		return err
	}

	endTime := time.Now()
	log.Info("tichecker online check all table finished", zap.Duration("cost", endTime.Sub(startTime)))
	return nil
}
