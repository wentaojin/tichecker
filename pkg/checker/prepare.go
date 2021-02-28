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

	// 清理断点表以及失败记录表，防止之前程序异常退出残留记录
	if err := db.TruncateMySQLTableRecord(); err != nil {
		return err
	}

	// 清理移除表配置文件输出位目录以及表检查结果输出目录
	if err := other.RemovePath(cfg.SyncDiffInspectorConfig.ConfigOutputDir); err != nil {
		return err
	}
	if err := other.RemovePath(cfg.SyncDiffInspectorConfig.FixSQLDir); err != nil {
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

	// 1、按表划分目录
	// 2、初始化 checkpoint 表记录
	// 3、生成 sync-diff-inspector 配置文件
	for _, tbl := range cfg.TiCheckerConfig.Tables {
		cfgPath := fmt.Sprintf("%s/%s/", cfg.SyncDiffInspectorConfig.ConfigOutputDir, tbl)
		if !other.FileAndDirIsExist(cfgPath) {
			log.Info("mkdir config output dir", zap.String("path", cfgPath))
			if err := other.TouchPath(cfgPath, os.ModePerm); err != nil {
				return err
			}
		}

		fixedPath := fmt.Sprintf("%s/%s/", cfg.SyncDiffInspectorConfig.FixSQLDir, tbl)
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
	}
	return nil
}

func startOnlineChecker(cfg *config.Cfg) error {
	wp := workpool.New(cfg.TiCheckerConfig.WorkerThreads)
	for _, table := range cfg.TiCheckerConfig.Tables {
		tbl := table
		wp.Do(func() error {
			startTime := time.Now()
			log.Info("tichecker online check start", zap.String("table", tbl))

			// 获取 checkpoint 数据
			rangSQLSlice, err := db.GetMySQLTableCheckpointRecordBySchemaTable(cfg.TiCheckerConfig.Schema, tbl)

			log.Info("maybe table sync diff nums", zap.Int("counts", len(rangSQLSlice)))
			if err != nil {
				return err
			}
			// 开始 sync-diff-inspector 检查
			for i := 0; i < len(rangSQLSlice); i++ {
				fileName := fmt.Sprintf("%s/%s/%s_diff%d.toml", cfg.SyncDiffInspectorConfig.ConfigOutputDir,
					tbl, tbl, i+1)
				ok, err := inspector.SyncDiffInspector(fileName)
				if err != nil {
					return err
				}
				// 如果检查失败，则重试
				if !ok {
					for j := 0; j <= cfg.TiCheckerConfig.CheckRetry; j++ {
						ok, err := inspector.SyncDiffInspector(fileName)
						if err != nil {
							return err
						}
						if ok {
							// 重试检查成功，跳出当前 for 循环，继续外层 for 循环
							goto Loop
						}
						// 间隔一段时间重试
						time.Sleep(time.Duration(cfg.TiCheckerConfig.TimeSleep) * time.Second)
					}

					// 检查失败，读取 fixed sql 文件并记录 sync_diff_inspector.failure 表
					fixedSQL := fmt.Sprintf(`%s/%s/%s_fixed%d.sql`,
						cfg.SyncDiffInspectorConfig.FixSQLDir,
						tbl, tbl, i+1)
					if err := db.WriteMySQLTableFailureRecord(cfg.TiCheckerConfig.Schema, tbl, fixedSQL); err != nil {
						return err
					}
				}
			Loop:
				continue
			}
			// 清理 checkpoint 记录
			if err := db.ClearMySQLTableCheckpointRecordBySchemaTable(cfg.TiCheckerConfig.Schema, tbl); err != nil {
				return err
			}

			endTime := time.Now()
			log.Info("tichecker online check finished", zap.String("table", tbl), zap.Duration("cost", endTime.Sub(startTime)))
			return nil
		})
	}
	if err := wp.Wait(); err != nil {
		return err
	}
	return nil
}
