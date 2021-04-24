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
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/wentaojin/tichecker/inspector"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	config = flag.String("config", "config.toml", "specify the configuration file, default is config.toml")
)

func main() {
	flag.Parse()

	cfg, err := inspector.NewConfig(*config)
	if err != nil {
		fmt.Printf("read config error: %v\n", err)
	}

	l := zap.NewAtomicLevel()
	if err := l.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		log.Error("invalid log level", zap.String("log level", cfg.LogLevel))
	}
	log.SetLevel(l.Level())

	ok := cfg.CheckConfig()
	if !ok {
		log.Fatal("there is something wrong with your config, please check it")
		os.Exit(1)
	}

	log.Info("tichecker start", zap.String("config", cfg.String()))

	ctx := context.Background()
	if !CheckSyncState(ctx, cfg) {
		log.Warn("check failed!!!")
		os.Exit(1)
	}
	log.Info("check pass!!!")
}

func CheckSyncState(ctx context.Context, cfg *inspector.Config) bool {
	beginTime := time.Now()
	defer func() {
		log.Info("check data finished", zap.Duration("cost", time.Since(beginTime)))
	}()

	d, err := inspector.NewDiff(ctx, cfg)
	if err != nil {
		log.Fatal("fail to initialize diff process", zap.Error(err))
	}

	err = d.Equal()
	if err != nil {
		log.Fatal("check data difference failed", zap.Error(err))
	}

	d.Report.Print()

	return d.Report.Result == inspector.Pass
}
