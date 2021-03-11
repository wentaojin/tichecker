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
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/WentaoJin/tichecker/pkg/diff"
	"github.com/WentaoJin/tichecker/pkg/utils"

	"github.com/WentaoJin/tichecker/pkg/dbutil"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/WentaoJin/tichecker/pkg/router"
	tidbconfig "github.com/pingcap/tidb/config"
)

// Diff contains two sql DB, used for comparing.
type Diff struct {
	CheckpointSchema  string
	SourceDBs         map[string]DBConfig
	TargetDB          DBConfig
	ChunkSize         int
	Sample            int
	CheckThreadCount  int
	CheckChunkThreads int
	FailedRetryTimes  int
	FailedRetrySleep  int
	UseChecksum       bool
	UseCheckpoint     bool
	OnlyUseChecksum   bool
	IgnoreDataCheck   bool
	IgnoreStructCheck bool
	IgnoreStats       bool
	FixSQLFile        *os.File

	Tables map[string]map[string]*TableConfig

	Report         *Report
	TidbInstanceID string
	TableRouter    *router.Table

	CheckpointDBConn *sql.DB

	Ctx context.Context
}

// NewDiff returns a Diff instance.
func NewDiff(ctx context.Context, cfg *Config) (diff *Diff, err error) {
	diff = &Diff{
		CheckpointSchema:  cfg.CheckpointSchema,
		SourceDBs:         make(map[string]DBConfig),
		ChunkSize:         cfg.ChunkSize,
		FailedRetryTimes:  cfg.FailedRetryTimes,
		FailedRetrySleep:  cfg.FailedRetrySleep,
		Sample:            cfg.Sample,
		CheckThreadCount:  cfg.CheckThreadCount,
		CheckChunkThreads: cfg.CheckChunkThreads,
		UseChecksum:       cfg.UseChecksum,
		UseCheckpoint:     cfg.UseCheckpoint,
		OnlyUseChecksum:   cfg.OnlyUseChecksum,
		IgnoreDataCheck:   cfg.IgnoreDataCheck,
		IgnoreStructCheck: cfg.IgnoreStructCheck,
		IgnoreStats:       cfg.IgnoreStats,
		Report:            NewReport(),
		Ctx:               ctx,
	}

	if err = diff.init(cfg); err != nil {
		diff.Close()
		return nil, errors.Trace(err)
	}

	return diff, nil
}

func (df *Diff) init(cfg *Config) (err error) {
	setTiDBCfg()

	// create connection for source and target.
	if err = df.CreateDBConn(cfg); err != nil {
		return errors.Trace(err)
	}

	if err = df.AdjustTableConfig(cfg); err != nil {
		return errors.Trace(err)
	}

	df.FixSQLFile, err = os.Create(cfg.FixSQLFile)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal() (err error) {
	defer df.Close()

	for _, schema := range df.Tables {
		for _, table := range schema {
			sourceTables := make([]*diff.TableInstance, 0, len(table.SourceTables))
			for _, sourceTable := range table.SourceTables {
				sourceTableInstance := &diff.TableInstance{
					Conn:       df.SourceDBs[sourceTable.InstanceID].Conn,
					Schema:     sourceTable.Schema,
					Table:      sourceTable.Table,
					InstanceID: sourceTable.InstanceID,
				}
				sourceTables = append(sourceTables, sourceTableInstance)
			}

			targetTableInstance := &diff.TableInstance{
				Conn:       df.TargetDB.Conn,
				Schema:     table.Schema,
				Table:      table.Table,
				InstanceID: df.TargetDB.InstanceID,
			}

			// find tidb instance for getting statistical information to split chunk
			var tidbStatsSource *diff.TableInstance
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if !df.IgnoreStats {
				log.Info("use tidb stats to split chunks")
				isTiDB, err := dbutil.IsTiDB(ctx, targetTableInstance.Conn)
				if err != nil {
					log.Warn("judge instance is tidb failed", zap.Error(err))
				} else if isTiDB {
					tidbStatsSource = targetTableInstance
				} else if len(sourceTables) == 1 {
					isTiDB, err := dbutil.IsTiDB(ctx, sourceTables[0].Conn)
					if err != nil {
						log.Warn("judge instance is tidb failed", zap.Error(err))
					} else if isTiDB {
						tidbStatsSource = sourceTables[0]
					}
				}
			} else {
				log.Info("ignore tidb stats because of user setting")
			}

			td := &diff.TableDiff{
				CheckpointSchema: df.CheckpointSchema,
				SourceTables:     sourceTables,
				TargetTable:      targetTableInstance,

				IgnoreColumns: table.IgnoreColumns,

				Fields:            table.Fields,
				Range:             table.Range,
				Collation:         table.Collation,
				ChunkSize:         df.ChunkSize,
				Sample:            df.Sample,
				CheckThreadCount:  df.CheckThreadCount,
				CheckChunkThreads: df.CheckChunkThreads,
				FailedRetryTimes:  df.FailedRetryTimes,
				FailedRetrySleep:  df.FailedRetrySleep,
				UseChecksum:       df.UseChecksum,
				UseCheckpoint:     df.UseCheckpoint,
				OnlyUseChecksum:   df.OnlyUseChecksum,
				IgnoreStructCheck: df.IgnoreStructCheck,
				IgnoreDataCheck:   df.IgnoreDataCheck,
				TiDBStatsSource:   tidbStatsSource,
				CheckpointDBConn:  df.CheckpointDBConn,
			}

			// table data check whether is equal or not equal
			structEqual, dataEqual, err := td.Equal(df.Ctx, func(dml string) error {
				_, err := df.FixSQLFile.WriteString(fmt.Sprintf("%s\n", dml))
				return errors.Trace(err)
			})

			if err != nil {
				log.Error("check failed", zap.String("table", dbutil.TableName(table.Schema, table.Table)), zap.Error(err))
				df.Report.SetTableMeetError(table.Schema, table.Table, err)
				df.Report.FailedNum++
				continue
			}

			df.Report.SetTableStructCheckResult(table.Schema, table.Table, structEqual)
			df.Report.SetTableDataCheckResult(table.Schema, table.Table, dataEqual)
			if structEqual && dataEqual {
				df.Report.PassNum++
			} else {
				df.Report.FailedNum++
			}
		}
	}

	return
}

// CreateDBConn creates db connections for source and target.
func (df *Diff) CreateDBConn(cfg *Config) (err error) {
	// create connection for source.
	for _, source := range cfg.SourceDBCfg {
		source.Conn, err = dbutil.CreateDB(df.Ctx, source.DBConfig, cfg.CheckThreadCount)
		if err != nil {
			return errors.Errorf("create source db %s error %v", source.DBConfig.String(), err)
		}
		df.SourceDBs[source.InstanceID] = source
	}

	// create connection for target.
	cfg.TargetDBCfg.Conn, err = dbutil.CreateDB(df.Ctx, cfg.TargetDBCfg.DBConfig, cfg.CheckThreadCount)
	if err != nil {
		return errors.Errorf("create target db %s error %v", cfg.TargetDBCfg.DBConfig.String(), err)
	}
	df.TargetDB = cfg.TargetDBCfg

	df.CheckpointDBConn, err = dbutil.CreateDBForCheckpoint(df.Ctx, cfg.TargetDBCfg.DBConfig)
	if err != nil {
		return errors.Errorf("create checkpoint db %s error %v", cfg.TargetDBCfg.DBConfig.String(), err)
	}

	return nil
}

// AdjustTableConfig adjusts the table's config by check-tables and table-config.
func (df *Diff) AdjustTableConfig(cfg *Config) (err error) {
	df.TableRouter, err = router.NewTableRouter(false, cfg.TableRules)
	if err != nil {
		return errors.Trace(err)
	}

	allTablesMap, err := df.GetAllTables()
	if err != nil {
		return errors.Trace(err)
	}

	// get all source table's matched target table
	// target database name => target table name => all matched source table instance
	sourceTablesMap := make(map[string]map[string][]TableInstance)
	for instanceID, allSchemas := range allTablesMap {
		if instanceID == df.TargetDB.InstanceID {
			continue
		}

		for schema, allTables := range allSchemas {
			for table := range allTables {
				targetSchema, targetTable, err := df.TableRouter.Route(schema, table)
				if err != nil {
					return errors.Errorf("get route result for %s.%s.%s failed, error %v", instanceID, schema, table, err)
				}

				if _, ok := sourceTablesMap[targetSchema]; !ok {
					sourceTablesMap[targetSchema] = make(map[string][]TableInstance)
				}

				if _, ok := sourceTablesMap[targetSchema][targetTable]; !ok {
					sourceTablesMap[targetSchema][targetTable] = make([]TableInstance, 0, 1)
				}

				sourceTablesMap[targetSchema][targetTable] = append(sourceTablesMap[targetSchema][targetTable], TableInstance{
					InstanceID: instanceID,
					Schema:     schema,
					Table:      table,
				})
			}
		}
	}

	// fill the table information.
	// will add default source information, don't worry, we will use table config's info replace this later.
	df.Tables = make(map[string]map[string]*TableConfig)
	for _, schemaTables := range cfg.Tables {
		df.Tables[schemaTables.Schema] = make(map[string]*TableConfig)
		tables := make([]string, 0, len(schemaTables.Tables))
		allTables, ok := allTablesMap[df.TargetDB.InstanceID][schemaTables.Schema]
		if !ok {
			return errors.NotFoundf("schema %s.%s", df.TargetDB.InstanceID, schemaTables.Schema)
		}

		for _, table := range schemaTables.Tables {
			matchedTables, err := df.GetMatchTable(df.TargetDB, schemaTables.Schema, table, allTables)
			if err != nil {
				return errors.Trace(err)
			}

			//exclude those in "exclude-tables"
			for _, t := range matchedTables {
				if df.InExcludeTables(schemaTables.ExcludeTables, t) {
					continue
				} else {
					tables = append(tables, t)
				}
			}
		}

		for _, tableName := range tables {
			tableInfo, err := dbutil.GetTableInfo(df.Ctx, df.TargetDB.Conn, schemaTables.Schema, tableName)
			if err != nil {
				return errors.Errorf("get table %s.%s's information error %s", schemaTables.Schema, tableName, errors.ErrorStack(err))
			}

			if _, ok := df.Tables[schemaTables.Schema][tableName]; ok {
				log.Error("duplicate config for one table", zap.String("table", dbutil.TableName(schemaTables.Schema, tableName)))
				continue
			}

			sourceTables := make([]TableInstance, 0, 1)
			if _, ok := sourceTablesMap[schemaTables.Schema][tableName]; ok {
				log.Info("find matched source tables", zap.Reflect("source tables", sourceTablesMap[schemaTables.Schema][tableName]), zap.String("target schema", schemaTables.Schema), zap.String("table", tableName))
				sourceTables = sourceTablesMap[schemaTables.Schema][tableName]
			} else {
				// use same database name and table name
				sourceTables = append(sourceTables, TableInstance{
					InstanceID: cfg.SourceDBCfg[0].InstanceID,
					Schema:     schemaTables.Schema,
					Table:      tableName,
				})
			}

			df.Tables[schemaTables.Schema][tableName] = &TableConfig{
				TableInstance: TableInstance{
					Schema: schemaTables.Schema,
					Table:  tableName,
				},
				TargetTableInfo: tableInfo,
				Range:           "TRUE",
				SourceTables:    sourceTables,
			}
		}
	}

	for _, table := range cfg.TableCfgs {
		if _, ok := df.Tables[table.Schema]; !ok {
			return errors.NotFoundf("schema %s in check tables", table.Schema)
		}
		if _, ok := df.Tables[table.Schema][table.Table]; !ok {
			return errors.NotFoundf("table %s.%s in check tables", table.Schema, table.Table)
		}

		sourceTables := make([]TableInstance, 0, len(table.SourceTables))
		for _, sourceTable := range table.SourceTables {
			if _, ok := df.SourceDBs[sourceTable.InstanceID]; !ok {
				return errors.Errorf("unknown database instance id %s", sourceTable.InstanceID)
			}

			allTables, ok := allTablesMap[df.SourceDBs[sourceTable.InstanceID].InstanceID][sourceTable.Schema]
			if !ok {
				return errors.Errorf("unknown schema %s in database %+v", sourceTable.Schema, df.SourceDBs[sourceTable.InstanceID])
			}

			tables, err := df.GetMatchTable(df.SourceDBs[sourceTable.InstanceID], sourceTable.Schema, sourceTable.Table, allTables)
			if err != nil {
				return errors.Trace(err)
			}

			for _, table := range tables {
				sourceTables = append(sourceTables, TableInstance{
					InstanceID: sourceTable.InstanceID,
					Schema:     sourceTable.Schema,
					Table:      table,
				})
			}
		}

		if len(sourceTables) != 0 {
			df.Tables[table.Schema][table.Table].SourceTables = sourceTables
		}
		if table.Range != "" {
			df.Tables[table.Schema][table.Table].Range = table.Range
		}
		df.Tables[table.Schema][table.Table].IgnoreColumns = table.IgnoreColumns
		df.Tables[table.Schema][table.Table].Fields = table.Fields
		df.Tables[table.Schema][table.Table].Collation = table.Collation
	}
	// we need to increase max open connections for upstream, because one chunk needs accessing N shard tables in one
	// upstream, and there are `CheckThreadCount` processing chunks. At most we need N*`CheckThreadCount` connections
	// for an upstream
	// instanceID -> max number of upstream shard tables every target table
	maxNumShardTablesOneRun := map[string]int{}
	for _, targetTables := range df.Tables {
		for _, sourceCfg := range targetTables {
			upstreamCount := map[string]int{}
			for _, sourceTables := range sourceCfg.SourceTables {
				upstreamCount[sourceTables.InstanceID]++
			}
			for id, count := range upstreamCount {
				if count > maxNumShardTablesOneRun[id] {
					maxNumShardTablesOneRun[id] = count
				}
			}
		}
	}

	for instanceId, count := range maxNumShardTablesOneRun {
		db := df.SourceDBs[instanceId].Conn
		if db == nil {
			return errors.Errorf("didn't found sourceDB for instance %s", instanceId)
		}
		log.Info("will increase connection configurations for DB of instance",
			zap.String("instance id", instanceId),
			zap.Int("connection limit", count*df.CheckThreadCount))
		db.SetMaxOpenConns(count * df.CheckThreadCount)
		db.SetMaxIdleConns(count * df.CheckThreadCount)
	}

	return nil
}

// GetAllTables get all tables in all databases.
func (df *Diff) GetAllTables() (map[string]map[string]map[string]interface{}, error) {
	// instanceID => schema => table
	allTablesMap := make(map[string]map[string]map[string]interface{})

	allTablesMap[df.TargetDB.InstanceID] = make(map[string]map[string]interface{})
	targetSchemas, err := dbutil.GetSchemas(df.Ctx, df.TargetDB.Conn)
	if err != nil {
		return nil, errors.Annotatef(err, "get schemas from %s", df.TargetDB.InstanceID)
	}
	for _, schema := range targetSchemas {
		allTables, err := dbutil.GetTables(df.Ctx, df.TargetDB.Conn, schema)
		if err != nil {
			return nil, errors.Annotatef(err, "get tables from %s.%s", df.TargetDB.InstanceID, schema)
		}
		allTablesMap[df.TargetDB.InstanceID][schema] = utils.SliceToMap(allTables)
	}

	for _, source := range df.SourceDBs {
		allTablesMap[source.InstanceID] = make(map[string]map[string]interface{})
		sourceSchemas, err := dbutil.GetSchemas(df.Ctx, source.Conn)
		if err != nil {
			return nil, errors.Annotatef(err, "get schemas from %s", source.InstanceID)
		}

		for _, schema := range sourceSchemas {
			allTables, err := dbutil.GetTables(df.Ctx, source.Conn, schema)
			if err != nil {
				return nil, errors.Annotatef(err, "get tables from %s.%s", source.InstanceID, schema)
			}
			allTablesMap[source.InstanceID][schema] = utils.SliceToMap(allTables)
		}
	}

	return allTablesMap, nil
}

// GetMatchTable returns all the matched table.
func (df *Diff) GetMatchTable(db DBConfig, schema, table string, allTables map[string]interface{}) ([]string, error) {
	tableNames := make([]string, 0, 1)

	if table[0] == '~' {
		tableRegex := regexp.MustCompile(fmt.Sprintf("(?i)%s", table[1:]))
		for tableName := range allTables {
			if !tableRegex.MatchString(tableName) {
				continue
			}
			tableNames = append(tableNames, tableName)
		}
	} else {
		if _, ok := allTables[table]; ok {
			tableNames = append(tableNames, table)
		} else {
			return nil, errors.Errorf("%s.%s not found in %s", schema, table, db.InstanceID)
		}
	}

	return tableNames, nil
}

// Close closes file and database connection.
func (df *Diff) Close() {
	if df.FixSQLFile != nil {
		df.FixSQLFile.Close()
	}

	for _, db := range df.SourceDBs {
		if db.Conn != nil {
			db.Conn.Close()
		}
	}

	if df.TargetDB.Conn != nil {
		df.TargetDB.Conn.Close()
	}

	if df.CheckpointDBConn != nil {
		df.CheckpointDBConn.Close()
	}
}

// Judge if a table is in "exclude-tables" list
func (df *Diff) InExcludeTables(excludeTables []string, table string) bool {
	for _, excludeTable := range excludeTables {
		if strings.EqualFold(excludeTable, table) {
			return true
		}
	}
	return false
}

func setTiDBCfg() {
	// to support long index key in TiDB
	tidbCfg := tidbconfig.GetGlobalConfig()
	// 3027 * 4 is the max value the MaxIndexLength can be set
	tidbCfg.MaxIndexLength = 3027 * 4
	tidbconfig.StoreGlobalConfig(tidbCfg)

	log.Info("set tidb cfg")
}
