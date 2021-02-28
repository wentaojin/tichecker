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
	"io/ioutil"
	"os"
	"strconv"
	"strings"
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
	querySQL = fmt.Sprintf(`TRUNCATE TABLE sync_diff_inspector.chunk`)
	log.Info("sql run", zap.String("sql", querySQL))
	if _, err := Engine.Exec(querySQL); err != nil {
		return err
	}

	querySQL = fmt.Sprintf(`TRUNCATE TABLE sync_diff_inspector.summary`)
	log.Info("sql run", zap.String("sql", querySQL))
	if _, err := Engine.Exec(querySQL); err != nil {
		return err
	}
	return nil
}

// 初始化业务表检查点记录
func InitMySQLTableCheckpointRecord(cfg *config.Cfg, tableName string) error {
	sql, err := generateMySQLTableCheckpointReplaceSQL(cfg.TiCheckerConfig.Schema,
		tableName,
		cfg.TiCheckerConfig.BaseTableColumn,
		"",
		cfg.TiCheckerConfig.SplitTableRows,
		cfg.TiCheckerConfig.WorkerThreads)
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
func WriteMySQLTableFailureRecord(schema, tableName, fileName string) error {
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	// 去除换行符并转移单引号问题
	sqlText := strings.Replace(string(bytes), "\n", "", -1)
	sqlText = strings.Replace(sqlText, "'", "\\'", -1)

	querySQL := fmt.Sprintf(`REPLACE INTO sync_diff_inspector.failure (schema_name,table_name,fix_sql) VALUES ('%s','%s','%s')`,
		schema, tableName, sqlText)
	log.Info("sql run", zap.String("sql", querySQL))
	if _, err := Engine.Exec(querySQL); err != nil {
		return err
	}
	return nil
}

// 查询 failure 失败记录
// 1、如果存在记录，说明可能存在上下游不一致，也可能因同步慢导致的，需要手工再验证下或者调大重试次数或者等待时间再校验
// 2、如果不存在记录，说明上下游数据一致
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
	var tmpCfg []TemplateCfg
	for i, rs := range rangSQLSlice {
		tcfg := TemplateCfg{
			ChunkSize:         cfg.InspectorConfig.ChunkSize,
			CheckThreadsCount: cfg.InspectorConfig.CheckThreadsCount,
			FixSQLFile: fmt.Sprintf("%s/%s/%s_fixed%d.sql",
				cfg.InspectorConfig.FixSQLDir,
				tableName, tableName, i+1),
			RangCond: rs,
		}
		log.Info("sync-diff-inspector template config file", zap.String("template", tcfg.String()))
		tmpCfg = append(tmpCfg, tcfg)
	}

	log.Info("maybe generate table diff toml file", zap.Int("counts", len(tmpCfg)))
	for i, toml := range tmpCfg {
		tmplFile := fmt.Sprintf("%s/%s_diff.tmpl", cfg.InspectorConfig.ConfigTemplateDir,
			tableName)
		outputFile := fmt.Sprintf("%s/%s/%s_diff%d.toml", cfg.InspectorConfig.ConfigOutputDir,
			tableName,
			tableName,
			i+1)
		if err := generateSyncDiffInspectorCfg(tmplFile, outputFile, toml); err != nil {
			return err
		}
	}
	return nil
}

func generateSyncDiffInspectorCfg(tmplFile string, outputFile string, tmpCfg TemplateCfg) error {
	if !other.FileAndDirIsExist(tmplFile) {
		return fmt.Errorf("sync-diff-inspector config file template [%v] not exist", tmplFile)
	}
	tmpl, err := template.ParseFiles(tmplFile)
	if err != nil {
		return err
	}
	//file, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY, 0755)
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
func generateMySQLTableCheckpointReplaceSQL(schema, tableName, colName, queryCond string, pageNums, workerThreads int) ([]string, error) {
	rangeSlice, err := getMySQLTableRecordByPage(schema, tableName, colName, queryCond, pageNums)
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
	sync_diff_inspector.checkpoint (schema_name,table_name,range_sql) VALUES ('%s','%s','%s')`, schema, tableName, rows))
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
func getMySQLTableRecordByPage(schema, tableName, colName, queryCond string, pageNums int) ([]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	%[1]s AS DATA
FROM
	( SELECT %[1]s, ROW_NUMBER ( ) OVER ( ORDER BY %[1]s ) AS RowNumber FROM ( SELECT DISTINCT %[1]s FROM %[2]s %[3]s) AS S ) AS T
WHERE
	RowNumber %% %[4]d = 0`, colName, fmt.Sprintf("%s.%s", schema, tableName), queryCond, pageNums)
	_, res, err := QueryRowsByPage(querySQL)
	if err != nil {
		return []string{}, err
	}
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
				rangSlice = append(rangSlice, fmt.Sprintf("%s >= %s", colName, s))
			} else {
				rangSlice = append(rangSlice, fmt.Sprintf("%s >= %s and %s < %s", colName, s, colName, sli[i+1]))
			}
		}
	case len(sli) == 1:
		rangSlice = append(rangSlice, fmt.Sprintf("%s >= %s", colName, sli[0]))
	}
	return rangSlice, nil
}
