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

import "C"
import (
	"flag"
	"os"

	"github.com/WentaoJin/tichecker/pkg/checker"
	"github.com/WentaoJin/tichecker/pkg/config"
	"github.com/WentaoJin/tichecker/pkg/signal"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	conf = flag.String("config", "tichecker.toml", "specify the configuration file, default is tichecker.toml")
)

func main() {
	flag.Parse()
	// 初始化日志
	l := zap.NewAtomicLevel()
	log.SetLevel(l.Level())

	// 读取配置文件
	cfg, err := config.ReadConfigFile(*conf)
	if err != nil {
		log.Fatal("read config file failed", zap.String("file", *conf), zap.String("error", err.Error()))
	}

	// 信号量监听处理
	signal.SetupSignalHandler(func(b bool) {
		os.Exit(0)
	})

	// 程序运行
	if err := checker.Run(cfg); err != nil {
		log.Fatal("server run failed", zap.Error(err))
	}
}
