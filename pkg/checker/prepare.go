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
	"fmt"
	"os"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/WentaoJin/tichecker/db"
	"github.com/WentaoJin/tichecker/inspector"
	"github.com/WentaoJin/tichecker/pkg/config"
	"github.com/WentaoJin/tichecker/pkg/other"
	"github.com/xxjwxc/gowp/workpool"
)

func prepareCheckerENV(cfg *config.Cfg) error {
	if err := db.NewMySQLEngine(cfg.TiCheckerConfig.DSN); err != nil {
		return err
	}

	// 重新初始化表结构
	if _, _, err := db.Query(db.Engine, db.SchemaDB); err != nil {
		return err
	}
	if _, _, err := db.Query(db.Engine, db.TableCheckpoint); err != nil {
		return err
	}
	if _, _, err := db.Query(db.Engine, db.TableFailure); err != nil {
		return err
	}

	// 清理断点表以及失败记录表，防止之前程序异常退出残留记录
	if err := db.ClearMySQLTableRecord(cfg.TiCheckerConfig.Schema, cfg.TiCheckerConfig.Tables); err != nil {
		return err
	}

	// 清理移除表配置文件输出位目录以及表检查结果输出目录
	for _, table := range cfg.TiCheckerConfig.Tables {
		if err := other.RemovePath(fmt.Sprintf(`%s/%s/`, cfg.InspectorConfig.ConfigOutputDir, table)); err != nil {
			return err
		}
		if err := other.RemovePath(fmt.Sprintf(`%s/%s/`, cfg.InspectorConfig.FixSQLDir, table)); err != nil {
			return err
		}
	}

	return nil
}

func generateInspectorConfig(cfg *config.Cfg) error {
	// 1、收集表统计信息
	// 2、按表划分目录
	// 2、初始化 checkpoint 表记录
	// 3、生成 sync-diff-inspector 配置文件
	wp := workpool.New(cfg.TiCheckerConfig.WorkerThreads)
	for _, table := range cfg.TiCheckerConfig.Tables {
		tbl := table
		wp.Do(func() error {

			if err := db.GatherMySQLTableStatistics(cfg.TiCheckerConfig.Schema, tbl); err != nil {
				return err
			}

			cfgPath := fmt.Sprintf("%s/%s/", cfg.InspectorConfig.ConfigOutputDir, tbl)
			if !other.FileAndDirIsExist(cfgPath) {
				log.Info("mkdir config output dir", zap.String("path", cfgPath))
				if err := other.TouchPath(cfgPath, os.ModePerm); err != nil {
					return err
				}
			}

			fixedPath := fmt.Sprintf("%s/%s/", cfg.InspectorConfig.FixSQLDir, tbl)
			if !other.FileAndDirIsExist(fixedPath) {
				log.Info("mkdir config fix-sql dir", zap.String("path", fixedPath))
				if err := other.TouchPath(fixedPath, os.ModePerm); err != nil {
					return err
				}
			}

			if err := db.InitMySQLTableCheckpointRecord(cfg, tbl); err != nil {
				return err
			}

			if err := db.GenerateSyncDiffInspectorCfg(cfg, tbl); err != nil {
				return err
			}
			return nil
		})
	}
	if err := wp.Wait(); err != nil {
		return err
	}
	return nil
}

func startOnlineChecker(cfg *config.Cfg) error {
	wp := workpool.New(cfg.TiCheckerConfig.WorkerThreads)
	for _, tbl := range cfg.TiCheckerConfig.Tables {
		table := tbl
		wp.Do(func() error {
			startTime := time.Now()
			log.Info("tichecker online check start", zap.String("table", table))
			// 获取 checkpoint 数据
			rangSQLSlice, err := db.GetMySQLTableCheckpointRecordBySchemaTable(cfg.TiCheckerConfig.Schema, table)
			if err != nil {
				return err
			}
			// 开始 sync-diff-inspector 检查
			for i, _ := range rangSQLSlice {
				idx := i
				// 检查失败，读取 fixed sql 文件并记录 sync_diff_inspector.failure 表
				configFileName := fmt.Sprintf("%s/%s/%s_diff%d.toml", cfg.InspectorConfig.ConfigOutputDir,
					table, table, idx)
				fixedFileName := fmt.Sprintf(`%s/%s/%s_fixed%d.sql`,
					cfg.InspectorConfig.FixSQLDir,
					table, table, idx)
				if err := failureRetry(
					cfg.TiCheckerConfig.Schema,
					table, configFileName, fixedFileName, cfg.TiCheckerConfig.CheckRetry, cfg.TiCheckerConfig.TimeSleep); err != nil {
					return err
				}
			}

			endTime := time.Now()
			// 清理 checkpoint 记录
			if err := db.ClearMySQLTableCheckpointRecordBySchemaTable(cfg.TiCheckerConfig.Schema, table); err != nil {
				return err
			}
			log.Info("tichecker online check finished", zap.String("table", table), zap.Duration("cost", endTime.Sub(startTime)))
			return nil
		})
	}
	if err := wp.Wait(); err != nil {
		return err
	}
	if !wp.IsDone() {
		log.Warn("tichecker online check failed, please manual check")
	}

	return nil
}

func failureRetry(schema, table, configFileName, fixedFileName string, maxRetryTimes, retrySleep int) error {
	for i := 0; i < (maxRetryTimes + 1); i++ {
		ok, err := inspector.SyncDiffInspector(configFileName)
		if err != nil {
			return err
		}
		if ok {
			break
		} else {
			if i == maxRetryTimes {
				if err := db.WriteMySQLTableFailureRecord(schema, table, configFileName, fixedFileName); err != nil {
					return err
				}
			} else {
				// 间隔一段时间重试
				time.Sleep(time.Duration(retrySleep) * time.Second)
				continue
			}
		}
	}
	return nil
}
