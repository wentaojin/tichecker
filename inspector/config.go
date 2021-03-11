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
package inspector

import (
	"database/sql"
	"encoding/json"
	"strconv"

	"github.com/pingcap/parser/model"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/WentaoJin/tichecker/pkg/router"

	"github.com/WentaoJin/tichecker/pkg/dbutil"
	"github.com/pingcap/log"
)

const (
	percent0   = 0
	percent100 = 100
)

var sourceInstanceMap map[string]interface{} = make(map[string]interface{})

// 配置数据库链接
type DBConfig struct {
	dbutil.DBConfig
	InstanceID string `toml:"instance-id" json:"instance-id"`
	Conn       *sql.DB
}

// 验证数据库配置是否有效
func (c *DBConfig) Valid() bool {
	if c.InstanceID == "" {
		log.Error("must specify source database's instance id")
		return false
	}
	sourceInstanceMap[c.InstanceID] = struct{}{}

	return true
}

// 目标端待检查表列表
type TargetTables struct {
	Schema        string   `toml:"schema" json:"schema"`
	Tables        []string `toml:"tables" json:"tables"`
	ExcludeTables []string `toml:"exclude-tables" json:"exclude-tables"`
}

// TableInstance saves the base information of table.
type TableInstance struct {
	// database's instance id
	InstanceID string `toml:"instance-id" json:"instance-id"`
	// schema name
	Schema string `toml:"schema"`
	// table name
	Table string `toml:"table"`
}

// TableConfig is the config of table.
type TableConfig struct {
	// table's origin information
	TableInstance
	// columns be ignored, will not check this column's data
	IgnoreColumns []string `toml:"ignore-columns"`
	// field should be the primary key, unique key or field with index
	Fields string `toml:"index-fields"`
	// select range, for example: "age > 10 AND age < 20"
	Range string `toml:"range"`
	// set true if comparing sharding tables with target table, should have more than one source tables.
	IsSharding bool `toml:"is-sharding"`
	// saves the source tables's info.
	// may have more than one source for sharding tables.
	// or you want to compare table with different schema and table name.
	// SourceTables can be nil when source and target is one-to-one correspondence.
	SourceTables    []TableInstance `toml:"source-tables"`
	TargetTableInfo *model.TableInfo

	// collation config in mysql/tidb
	Collation string `toml:"collation"`
}

// 配置文件
type Config struct {
	// log level
	LogLevel string `toml:"log-level" json:"log-level"`

	// checkpoint meta schema
	CheckpointSchema string `toml:"checkpoint-schema" json:"checkpoint-schema"`

	// source database's config
	SourceDBCfg []DBConfig `toml:"source-db" json:"source-db"`

	// target database's config
	TargetDBCfg DBConfig `toml:"target-db" json:"target-db"`

	// for example, the whole data is [1...100]
	// we can split these data to [1...10], [11...20], ..., [91...100]
	// the [1...10] is a chunk, and it's chunk size is 10
	// size of the split chunk
	ChunkSize int `toml:"chunk-size" json:"chunk-size"`

	// Chunk check failed, retry times
	FailedRetryTimes int `toml:"failed-retry-times" json:"failed-retry-times"`

	// Chunk retry sleep
	FailedRetrySleep int `toml:"failed-retry-sleep" json:"failed-retry-sleep"`

	// sampling check percent, for example 10 means only check 10% data
	Sample int `toml:"sample-percent" json:"sample-percent"`

	// how many goroutines are created to check data /chunk
	CheckThreadCount int `toml:"check-thread-count" json:"check-thread-count"`

	// when chun data isn't consistent how many goroutines are created to check chunk data
	// todo: no-used
	CheckChunkThreads int `toml:"check-chunk-threads" json:"check-chunk-threads"`

	// set false if want to comapre the data directly
	UseChecksum bool `toml:"use-checksum" json:"use-checksum"`

	// set true if just want compare data by checksum, will skip select data when checksum is not equal.
	OnlyUseChecksum bool `toml:"only-use-checksum" json:"only-use-checksum"`

	// the name of the file which saves sqls used to fix different data
	FixSQLFile string `toml:"fix-sql-file" json:"fix-sql-file"`

	// the tables to be checked
	Tables []*TargetTables `toml:"check-tables" json:"check-tables"`

	// the config of table
	TableCfgs []*TableConfig `toml:"table-config" json:"table-config"`

	// TableRules defines table name and database name's conversion relationship between source database and target database
	TableRules []*router.TableRule `toml:"table-rules" json:"table-rules"`

	// ignore check table's struct
	IgnoreStructCheck bool `toml:"ignore-struct-check" json:"ignore-struct-check"`

	// ignore tidb stats only use randomSpliter to split chunks
	IgnoreStats bool `toml:"ignore-stats" json:"ignore-stats"`

	// ignore check table's data
	IgnoreDataCheck bool `toml:"ignore-data-check" json:"ignore-data-check"`

	// set true will continue check from the latest checkpoint
	UseCheckpoint bool `toml:"use-checkpoint" json:"use-checkpoint"`
}

// 创建配置文件
func NewConfig(file string) (*Config, error) {
	cfg := &Config{}
	if err := cfg.configFromFile(file); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	meta, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.Trace(err)
	}
	if len(meta.Undecoded()) > 0 {
		return errors.Errorf("unknown keys in config file %s: %v", path, meta.Undecoded())
	}
	return nil
}

func (c *Config) CheckConfig() bool {
	if c.Sample > percent100 || c.Sample < percent0 {
		log.Error("sample must be greater than 0 and less than or equal to 100!")
		return false
	}

	if c.CheckThreadCount <= 0 {
		log.Error("check-thread-count must greater than 0!")
		return false
	}

	if c.FailedRetryTimes <= 0 {
		log.Error("failed-retry-times must greater than 0!")
		return false
	}

	if c.FailedRetrySleep <= 0 {
		log.Error("failed-retry-sleep must greater than 0!")
		return false
	}

	// todo: Used to optimize the data verification in the chunk
	// todo: Not yet Do ,Save default But NoUsed
	if c.CheckChunkThreads <= 0 {
		//log.Error("check-chunk-threads must greater than 0!")
		//return false
		c.CheckChunkThreads = 4
	}

	if len(c.CheckpointSchema) == 0 {
		log.Error("checkpoint-schema must have checkpoint schema database")
		return false
	}

	if len(c.SourceDBCfg) == 0 {
		log.Error("must have at least one source database")
		return false
	}

	for i := range c.SourceDBCfg {
		if !c.SourceDBCfg[i].Valid() {
			return false
		}
		if c.SourceDBCfg[i].Snapshot != "" {
			c.SourceDBCfg[i].Snapshot = strconv.Quote(c.SourceDBCfg[i].Snapshot)
		}
	}

	if c.TargetDBCfg.InstanceID == "" {
		c.TargetDBCfg.InstanceID = "target"
	}
	if c.TargetDBCfg.Snapshot != "" {
		c.TargetDBCfg.Snapshot = strconv.Quote(c.TargetDBCfg.Snapshot)
	}
	if _, ok := sourceInstanceMap[c.TargetDBCfg.InstanceID]; ok {
		log.Error("target has same instance id in source", zap.String("instance id", c.TargetDBCfg.InstanceID))
		return false
	}

	if len(c.Tables) == 0 {
		log.Error("must specify check tables")
		return false
	}

	if c.OnlyUseChecksum {
		if !c.UseChecksum {
			log.Error("need set use-checksum = true")
			return false
		}
	} else {
		if len(c.FixSQLFile) == 0 {
			log.Warn("fix-sql-file is invalid, will use default value 'fix.sql'")
			c.FixSQLFile = "fix.sql"
		}
	}

	return true
}
