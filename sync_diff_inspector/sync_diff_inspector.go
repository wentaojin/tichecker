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
package sync_diff_inspector

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func SyncDiffInspector(fileName string) (bool, error) {
	cfg, err := NewConfig(fileName)
	if err != nil {
		return false, err
	}

	l := zap.NewAtomicLevel()
	if err := l.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		log.Error("invalide log level", zap.String("log level", cfg.LogLevel))
		return false, fmt.Errorf("sync-diff-inspector set invalide log level failed: %v", err)
	}
	log.SetLevel(l.Level())

	ok := cfg.checkConfig()
	if !ok {
		return false, fmt.Errorf("there is something wrong with your config, please check it")
	}
	log.Info("sync-diff-inspector start", zap.String("config", cfg.String()))

	ctx := context.Background()
	if !checkSyncState(ctx, cfg) {
		log.Warn("check failed!!!")
		return false, nil
	} else {
		log.Info("check pass!!!")
		return true, nil
	}
}

func checkSyncState(ctx context.Context, cfg *Config) bool {
	beginTime := time.Now()
	defer func() {
		log.Info("check data finished", zap.Duration("cost", time.Since(beginTime)))
	}()

	d, err := NewDiff(ctx, cfg)
	if err != nil {
		log.Fatal("fail to initialize diff process", zap.Error(err))
	}

	err = d.Equal()
	if err != nil {
		log.Fatal("check data difference failed", zap.Error(err))
	}

	d.report.Print()

	return d.report.Result == Pass
}
