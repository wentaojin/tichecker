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
package config

import (
	"encoding/json"
	"fmt"

	"github.com/BurntSushi/toml"
)

type Cfg struct {
	TiCheckerConfig TiCheckerConfig `toml:"tichecker" json:"tichecker"`
	InspectorConfig InspectorConfig `toml:"inspector" json:"inspector"`
}

type TiCheckerConfig struct {
	DSN             string   `toml:"dsn" json:"dsn"`
	Schema          string   `toml:"schema" json:"schema"`
	Tables          []string `toml:"tables",json:"tables"`
	BaseTableColumn string   `toml:"base-table-column" json:"base-table-column"`
	IsIndexColumn   bool     `toml:"is-index-column" json:"is-index-column "`
	SplitTableRows  int      `toml:"split-table-rows" json:"split-table-rows"`
	WorkerThreads   int      `toml:"worker-threads" json:"worker-threads"`
	TimeSleep       int      `toml:"time-sleep" json:"time-sleep"`
	CheckRetry      int      `toml:"check-retry" json:"check-retry"`
}

type InspectorConfig struct {
	ConfigTemplateDir string `toml:"config-template-dir" json:"config-template-dir"`
	ConfigOutputDir   string `toml:"config-output-dir" json:"config-output-dir"`
	FixSQLDir         string `toml:"fix-sql-dir" json:"fix-sql-dir"`
	CheckThreadsCount int    `toml:"check-thread-count" json:"check-thread-count"`
	ChunkSize         int    `toml:"chunk-size" json:"chunk-size"`
}

// 读取配置文件
func ReadConfigFile(file string) (*Cfg, error) {
	cfg := &Cfg{}
	if err := cfg.configFromFile(file); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// 加载配置文件并解析
func (c *Cfg) configFromFile(file string) error {
	if _, err := toml.DecodeFile(file, c); err != nil {
		return fmt.Errorf("failed decode toml config file %s: %v", file, err)
	}
	return nil
}

func (c *Cfg) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}
