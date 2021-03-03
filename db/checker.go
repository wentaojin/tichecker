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
package db

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"text/template"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/WentaoJin/tichecker/pkg/config"
	"github.com/WentaoJin/tichecker/pkg/other"

	"github.com/xxjwxc/gowp/workpool"
)

// 获取 checkpoint 表记录
func GetMySQLTableCheckpointRecordBySchemaTable(schema, tableName string) ([]string, error) {
	_, res, err := Query(Engine, fmt.Sprintf(`SELECT RANGE_SQL FROM sync_diff_inspector.checkpoint WHERE upper(schema_name) = upper('%s') AND upper(table_name) = upper('%s')`,
		schema, tableName))
	if err != nil {
		return []string{}, err
	}
	var rangeSQL []string
	for _, r := range res {
		rangeSQL = append(rangeSQL, r["RANGE_SQL"])
	}
	return rangeSQL, nil
}

// 清理 checkpoint 表记录
func ClearMySQLTableCheckpointRecordBySchemaTable(schema, tableName string) error {
	querySQL := fmt.Sprintf(`DELETE FROM sync_diff_inspector.checkpoint WHERE upper(schema_name) = upper('%s') AND upper(table_name) = upper('%s')`,
		schema, tableName)
	log.Info("sql run", zap.String("sql", querySQL))
	if _, err := Engine.Exec(querySQL); err != nil {
		return err
	}
	return nil
}

// 收集表统计信息，用于 inspector 划分 chunk
func GatherMySQLTableStatistics(schema, tableName string) error {
	querySQL := fmt.Sprintf(`ANALYZE TABLE %s.%s`, schema, tableName)
	log.Info("sql run", zap.String("sql", querySQL))
	if _, err := Engine.Query(querySQL); err != nil {
		return err
	}
	return nil
}

// truncate checkpoint 表记录
func TruncateMySQLTableRecord() error {
	querySQL := fmt.Sprintf(`TRUNCATE TABLE sync_diff_inspector.checkpoint`)
	log.Info("sql run", zap.String("sql", querySQL))
	if _, err := Engine.Exec(querySQL); err != nil {
		return err
	}
	querySQL = fmt.Sprintf(`TRUNCATE TABLE sync_diff_inspector.failure`)
	log.Info("sql run", zap.String("sql", querySQL))
	if _, err := Engine.Exec(querySQL); err != nil {
		return err
	}

	querySQL = fmt.Sprintf(`select COUNT(1) AS COUNT
	from  INFORMATION_SCHEMA.TABLES  where upper(TABLE_SCHEMA) =upper('sync_diff_inspector') and  upper(TABLE_NAME) = upper('chunk')`)
	_, res, err := Query(Engine, querySQL)
	if err != nil {
		return err
	}
	if res[0]["COUNT"] != "0" {
		querySQL = fmt.Sprintf(`TRUNCATE TABLE sync_diff_inspector.chunk`)
		log.Info("sql run", zap.String("sql", querySQL))
		if _, err := Engine.Exec(querySQL); err != nil {
			return err
		}
	}

	querySQL = fmt.Sprintf(`select COUNT(1) AS COUNT
	from  INFORMATION_SCHEMA.TABLES  where upper(TABLE_SCHEMA) =upper('sync_diff_inspector') and  upper(TABLE_NAME) = upper('summary')`)
	_, res, err = Query(Engine, querySQL)
	if err != nil {
		return err
	}
	if res[0]["COUNT"] != "0" {
		querySQL = fmt.Sprintf(`TRUNCATE TABLE sync_diff_inspector.summary`)
		log.Info("sql run", zap.String("sql", querySQL))
		if _, err := Engine.Exec(querySQL); err != nil {
			return err
		}
	}
	return nil
}

// 初始化业务表检查点记录
func InitMySQLTableCheckpointRecord(cfg *config.Cfg, tableName string) error {
	sql, err := generateMySQLTableCheckpointReplaceSQL(cfg.TiCheckerConfig.Schema,
		tableName,
		cfg.TiCheckerConfig.BaseTableColumn,
		cfg.TiCheckerConfig.SplitTableRows,
		cfg.TiCheckerConfig.WorkerThreads,
		cfg.TiCheckerConfig.IsIndexColumn)
	if err != nil {
		return err
	}
	wp := workpool.New(cfg.TiCheckerConfig.WorkerThreads)

	for _, s := range sql {
		rs := s
		wp.Do(func() error {
			log.Info("sql run", zap.String("sql", rs))
			if _, err := Engine.Exec(rs); err != nil {
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

// 记录检查失败记录到表 failure
func WriteMySQLTableFailureRecord(schema, tableName, configFileName, fixFileName string) error {
	querySQL := fmt.Sprintf(`REPLACE INTO sync_diff_inspector.failure (schema_name,table_name,config_file,fix_file) VALUES ('%s','%s','%s','%s')`,
		schema, tableName, configFileName, fixFileName)
	log.Info("sql run", zap.String("sql", querySQL))
	if _, err := Engine.Exec(querySQL); err != nil {
		return err
	}
	// 考虑可能存在 SQL 太大超出字符直接去除忽略
	//bytes, err := ioutil.ReadFile(fileName)
	//if err != nil {
	//	return err
	//}
	// 当数据校验一致，则不写入 failure 表
	//if len(bytes) > 0 {
	//	// 去除换行符并转移单引号问题
	//	sqlText := strings.Replace(string(bytes), "\n", "", -1)
	//	sqlText = strings.Replace(sqlText, "'", "\\'", -1)
	//querySQL := fmt.Sprintf(`REPLACE INTO sync_diff_inspector.failure (schema_name,table_name,fix_file,fix_sql) VALUES ('%s','%s','%s','%s')`,
	//	schema, tableName, fileName, sqlText)
	//log.Info("sql run", zap.String("sql", querySQL))
	//if _, err := Engine.Exec(querySQL); err != nil {
	//	return err
	//}
	//}
	return nil
}

// 查询 failure 失败记录
// 1、如果存在记录，说明可能存在上下游不一致，也可能因同步慢导致的，需要手工再验证下或者调大重试次数或者等待时间再校验
// 2、如果不存在记录，说明上下游数据一致
type Failure struct {
	ID         int    `json:"id"`
	SchemaName string `json:"schema_name"`
	TableName  string `json:"table_name"`
	ConfigFile string `json:"config_file"`
	FixFile    string `json:"fix_file"`
	UpdateTime string `json:"update_time"`
	//FixSQL     string `json:"fix_sql"`
}

func GetMySQLTableFailureRecord() error {
	querySQL := fmt.Sprintf(`SELECT Count(1) AS COUNT FROM sync_diff_inspector.failure`)
	_, res, err := Query(Engine, querySQL)
	if err != nil {
		return err
	}
	ct, err := strconv.Atoi(res[0]["COUNT"])
	if err != nil {
		return err
	}
	if ct == 0 {
		log.Info("The upstream and downstream data are be consistent, all pass!!!")
		return nil
	} else {
		querySQL := fmt.Sprintf(`SELECT id,schema_name,table_name,config_file,fix_file,update_time FROM sync_diff_inspector.failure`)
		rows, err := Engine.Query(querySQL)
		if err != nil {
			return err
		}

		var warnRes []string
		for rows.Next() {
			failure := Failure{}
			err = rows.Scan(&failure.ID, &failure.SchemaName, &failure.TableName, &failure.ConfigFile, &failure.FixFile, &failure.UpdateTime)
			if err != nil {
				return err
			}
			b, err := json.Marshal(failure)
			if err != nil {
				return err
			}
			warnRes = append(warnRes, string(b))
		}
		log.Warn("The upstream and downstream data may be inconsistent", zap.String("msg", fmt.Sprintf("%v", warnRes)))
		log.Warn("The upstream and downstream data may be inconsistent, please manually check table failure record [sync_diff_inspector.failure] or adjust the configuration file retry times or wait time to re-check")
		log.Warn("tichecker failure record lay in the table [sync_diff_inspector.failure], re-run tichecker would be clear")
		return nil
	}
}

/*
	sync-diff-inspector 配置文件生成
*/
type TemplateCfg struct {
	ChunkSize         int
	CheckThreadsCount int
	FixSQLFile        string
	RangCond          string
}

func (c *TemplateCfg) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}

func GenerateSyncDiffInspectorCfg(cfg *config.Cfg, tableName string) error {
	// 获取 checkpoint 数据
	rangSQLSlice, err := GetMySQLTableCheckpointRecordBySchemaTable(cfg.TiCheckerConfig.Schema, tableName)
	if err != nil {
		return err
	}

	log.Info("maybe generate fixed sql file", zap.Int("counts", len(rangSQLSlice)))
	// 根据模板文件生成对应 sync-diff-inspector 配置文件
	for i, rs := range rangSQLSlice {
		tcfg := &TemplateCfg{
			ChunkSize:         cfg.InspectorConfig.ChunkSize,
			CheckThreadsCount: cfg.InspectorConfig.CheckThreadsCount,
			FixSQLFile: fmt.Sprintf("%s/%s/%s_fixed%d.sql",
				cfg.InspectorConfig.FixSQLDir,
				tableName, tableName, i),
			RangCond: rs,
		}
		log.Info("sync-diff-inspector template config file", zap.String("template", tcfg.String()))
		tmplFile := fmt.Sprintf("%s/%s_diff.tmpl", cfg.InspectorConfig.ConfigTemplateDir,
			tableName)
		outputFile := fmt.Sprintf("%s/%s/%s_diff%d.toml", cfg.InspectorConfig.ConfigOutputDir,
			tableName,
			tableName,
			i)
		log.Info("sync-diff-inspector toml config file", zap.String("template", tcfg.String()))
		if err := generateSyncDiffInspectorCfg(tmplFile, outputFile, tcfg); err != nil {
			return err
		}
	}
	return nil
}

func generateSyncDiffInspectorCfg(tmplFile string, outputFile string, tmpCfg *TemplateCfg) error {
	if !other.FileAndDirIsExist(tmplFile) {
		return fmt.Errorf("sync-diff-inspector config file template [%v] not exist", tmplFile)
	}
	tmpl, err := template.ParseFiles(tmplFile)
	if err != nil {
		return err
	}
	file, err := os.Create(outputFile)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("os open file [%v] failed: %v", outputFile, err)
	}
	err = tmpl.Execute(file, tmpCfg)
	if err != nil {
		return err
	}
	return nil
}

// 生成写入 checkpoint 表语句
func generateMySQLTableCheckpointReplaceSQL(schema, tableName, colName string, splitTableRows, workerThreads int, isIndexColumn bool) ([]string, error) {
	rangeSlice, err := getMySQLTableRecordByPage(schema, tableName, colName, splitTableRows, isIndexColumn)
	if err != nil {
		return []string{}, err
	}
	var sql []string

	// 保证并发 Slice Append 安全
	var lock sync.Mutex
	wp := workpool.New(workerThreads)

	for _, rg := range rangeSlice {
		rows := rg
		wp.Do(func() error {
			lock.Lock()
			sql = append(sql, fmt.Sprintf(`REPLACE INTO 
	sync_diff_inspector.checkpoint (schema_name,table_name,range_sql) VALUES ('%s','%s',"%s")`, schema, tableName, rows))
			lock.Unlock()
			return nil
		})
	}
	if err := wp.Wait(); err != nil {
		return sql, err
	}
	return sql, nil
}

// 根据 base-table-column 以及分页数获取下游 tidb 记录，写入检查 checkpoint 表
// 当前是基于 range 切分查询检验，需要有特殊需要，比如 distinct 特别少，但是数据变动少，可以根据需要更下函数逻辑，生成等值语句进行校验
func getMySQLTableRecordByPage(schema, tableName, columnName string, splitTableRows int, isIndexColumn bool) ([]string, error) {
	var querySQL string
	if isIndexColumn {
		querySQL = fmt.Sprintf(`SELECT
	%[1]s AS DATA
FROM
	( SELECT %[1]s, ROW_NUMBER ( ) OVER ( ORDER BY %[1]s ) AS RowNumber FROM ( SELECT DISTINCT %[1]s FROM %[2]s ORDER BY %[1]s) AS S ) AS T
WHERE
	RowNumber %% %[3]d = 0`, columnName, fmt.Sprintf("%s.%s", schema, tableName), splitTableRows)
	} else {
		querySQL = fmt.Sprintf(`SELECT
	%[1]s AS DATA
FROM
	( SELECT %[1]s, ROW_NUMBER ( ) OVER ( ORDER BY %[1]s ) AS RowNumber FROM ( SELECT DISTINCT %[1]s FROM %[2]s ) AS S ) AS T
WHERE
	RowNumber %% %[3]d = 0`, columnName, fmt.Sprintf("%s.%s", schema, tableName), splitTableRows)
	}
	_, res, err := QueryRows(querySQL)
	if err != nil {
		return []string{}, err
	}

	// 判断分页是否存在，如果不存在则取字段最大值 MAX，一次范围查询对比，如果存在划分范围对比
	if len(res) == 0 {
		var (
			rangSlice []string
		)
		querySQL = fmt.Sprintf(`SELECT MAX(%s) MAX_VALUE FROM %s.%s`, columnName, schema, tableName)
		_, res, err = QueryRows(querySQL)
		if err != nil {
			return []string{}, err
		}

		rangSlice = append(rangSlice, fmt.Sprintf("%s <= %s", columnName, res[0][0]))
		rangSlice = append(rangSlice, fmt.Sprintf("%s > %s", columnName, res[0][0]))
		return rangSlice, nil
	} else {
		var (
			sli       []string
			rangSlice []string
		)
		for _, row := range res {
			sli = append(sli, row[0])
		}

		// 生成 range 语句
		switch {
		case len(sli) > 1:
			for i, s := range sli {
				if i == len(sli)-1 {
					rangSlice = append(rangSlice, fmt.Sprintf("%s >= %s", columnName, s))
				} else if i == 0 {
					rangSlice = append(rangSlice, fmt.Sprintf("%s < %s", columnName, s))
					rangSlice = append(rangSlice, fmt.Sprintf("%s >= %s and %s < %s", columnName, s, columnName, sli[i+1]))
				} else {
					rangSlice = append(rangSlice, fmt.Sprintf("%s >= %s and %s < %s", columnName, s, columnName, sli[i+1]))
				}
			}
		case len(sli) == 1:
			rangSlice = append(rangSlice, fmt.Sprintf("%s < %s", columnName, sli[0]))
			rangSlice = append(rangSlice, fmt.Sprintf("%s >= %s", columnName, sli[0]))
		}
		return rangSlice, nil
	}
}
