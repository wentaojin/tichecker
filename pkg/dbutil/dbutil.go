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
package dbutil

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// DBConfig is database configuration.
type DBConfig struct {
	Host string `toml:"host" json:"host"`

	Port int `toml:"port" json:"port"`

	User string `toml:"user" json:"user"`

	Password string `toml:"password" json:"-"` // omit it for privacy

	Schema string `toml:"schema" json:"schema"`

	Snapshot string `toml:"snapshot" json:"snapshot"`
}

// String returns native format of database configuration
func (c *DBConfig) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}

// OpenDB opens a mysql connection FD
func OpenDB(cfg DBConfig) (*sql.DB, error) {
	var dbDSN string
	if len(cfg.Snapshot) != 0 {
		log.Info("create connection with snapshot", zap.String("snapshot", cfg.Snapshot))
		dbDSN = fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&tidb_snapshot=%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Snapshot)
	} else {
		dbDSN = fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	}

	dbConn, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = dbConn.Ping()
	return dbConn, errors.Trace(err)
}

// GetSchemas returns name of all schemas
func GetSchemas(ctx context.Context, db *sql.DB) ([]string, error) {
	query := "SHOW DATABASES"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	// show an example.
	/*
		mysql> SHOW DATABASES;
		+--------------------+
		| Database           |
		+--------------------+
		| information_schema |
		| mysql              |
		| performance_schema |
		| sys                |
		| test_db            |
		+--------------------+
	*/
	schemas := make([]string, 0, 10)
	for rows.Next() {
		var schema string
		err = rows.Scan(&schema)
		if err != nil {
			return nil, errors.Trace(err)
		}
		schemas = append(schemas, schema)
	}
	return schemas, errors.Trace(rows.Err())
}

// GetTables returns name of all tables in the specified schema
func GetTables(ctx context.Context, db *sql.DB, schemaName string) (tables []string, err error) {
	/*
		show tables without view: https://dev.mysql.com/doc/refman/5.7/en/show-tables.html
		example:
		mysql> show full tables in test where Table_Type != 'VIEW';
		+----------------+------------+
		| Tables_in_test | Table_type |
		+----------------+------------+
		| NTEST          | BASE TABLE |
		+----------------+------------+
	*/
	query := fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW';", escapeName(schemaName))
	return queryTables(ctx, db, query)
}

func queryTables(ctx context.Context, db *sql.DB, q string) (tables []string, err error) {
	log.Debug("query tables", zap.String("query", q))
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	tables = make([]string, 0, 8)
	for rows.Next() {
		var table, tType sql.NullString
		err = rows.Scan(&table, &tType)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if !table.Valid || !tType.Valid {
			continue
		}

		tables = append(tables, table.String)
	}

	return tables, errors.Trace(rows.Err())
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}
