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
	"database/sql"
	"fmt"

	"github.com/WentaoJin/tichecker/pkg/other"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var Engine *sql.DB

const (
	SchemaDB = `CREATE DATABASE IF NOT EXISTS sync_diff_inspector`

	TableCheckpoint = `CREATE TABLE
IF
	NOT EXISTS sync_diff_inspector.checkpoint (
	id BIGINT PRIMARY KEY NOT NULL auto_increment,
	schema_name VARCHAR ( 64 ) NOT NULL,
	table_name VARCHAR ( 64 ) NOT NULL,
	range_sql text NOT NULL,
	update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	INDEX idx_complex ( SCHEMA_NAME, TABLE_NAME ) 
	) ENGINE = INNODB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin`

	TableFailure = `CREATE TABLE
IF
	NOT EXISTS sync_diff_inspector.failure (
	id BIGINT PRIMARY KEY NOT NULL auto_increment,
	schema_name VARCHAR ( 64 ) NOT NULL,
	table_name VARCHAR ( 64 ) NOT NULL,
	fix_sql text NOT NULL,
	update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_complex ( SCHEMA_NAME, TABLE_NAME )
	) ENGINE = INNODB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin`
)

func NewMySQLEngine(dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("error on initializing mysql database connection [no-schema]: %v", err)
	}

	Engine = db
	Engine.SetMaxOpenConns(10)
	Engine.SetMaxIdleConns(10)
	return nil
}

// 查询返回表字段列和对应的字段行数据
func Query(db *sql.DB, querySQL string) ([]string, []map[string]string, error) {
	log.Info("sql run", zap.String("sql", querySQL))
	var (
		cols []string
		res  []map[string]string
	)
	rows, err := db.Query(querySQL)
	if err != nil {
		return cols, res, fmt.Errorf("[%v] error on general query SQL [%v] failed", err.Error(), querySQL)
	}
	defer rows.Close()

	//不确定字段通用查询，自动获取字段名称
	cols, err = rows.Columns()
	if err != nil {
		return cols, res, fmt.Errorf("[%v] error on general query rows.Columns failed", err.Error())
	}

	values := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return cols, res, fmt.Errorf("[%v] error on general query rows.Scan failed", err.Error())
		}

		row := make(map[string]string)
		for k, v := range values {
			key := cols[k]
			if v == nil {
				row[key] = "NULL"
			} else {
				// 处理空字符串以及其他值情况
				row[key] = string(v)
			}
		}
		res = append(res, row)
	}
	return cols, res, nil
}

// 专用于分页查询
func QueryRowsByPage(querySQL string) ([]string, [][]string, error) {
	log.Info("sql run", zap.String("sql", querySQL))
	var (
		cols []string
		err  error
	)
	rows, err := Engine.Query(querySQL)
	if err == nil {
		defer rows.Close()
	}
	if err != nil {
		return cols, [][]string{}, err
	}

	cols, err = rows.Columns()
	if err != nil {
		return cols, [][]string{}, err
	}

	// 用于判断字段值是数字还是字符
	var columnTypes []string
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return cols, [][]string{}, err
	}

	for _, ct := range colTypes {
		// 数据库字段类型 DatabaseTypeName() 映射 go 类型 ScanType()
		columnTypes = append(columnTypes, ct.ScanType().String())
	}

	// Read all rows
	var actualRows [][]string
	for rows.Next() {
		rawResult := make([][]byte, len(cols))
		result := make([]string, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err = rows.Scan(dest...)
		if err != nil {
			return cols, actualRows, err
		}

		for i, raw := range rawResult {
			// Mysql 空字符串与 NULL 非一类，NULL 是 NULL，空字符串是空字符串（is null 只查询 NULL 值，空字符串查询只查询到空字符串值）
			if raw == nil {
				result[i] = "NULL"
			} else if string(raw) == "" {
				result[i] = "NULL"
			} else {
				ok := other.IsNum(string(raw))
				switch {
				case ok && columnTypes[i] != "string":
					result[i] = string(raw)
				default:
					result[i] = fmt.Sprintf("'%s'", string(raw))
				}

			}
		}
		actualRows = append(actualRows, result)
	}

	if err = rows.Err(); err != nil {
		return cols, actualRows, err
	}

	return cols, actualRows, nil
}
