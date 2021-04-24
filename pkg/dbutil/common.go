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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/wentaojin/tichecker/pkg/utils"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"

	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	tmysql "github.com/pingcap/parser/mysql"
	"go.uber.org/zap"

	"github.com/pingcap/parser"
)

const (
	// DefaultRetryTime is the default retry time to execute sql
	DefaultRetryTime = 10

	// DefaultTimeout is the default timeout for execute sql
	DefaultTimeout time.Duration = 10 * time.Second

	// SlowLogThreshold defines the duration to log debug log of sql when exec time greater than
	SlowLogThreshold = 200 * time.Millisecond

	// DefaultDeleteRowsNum is the default rows num for delete one time
	DefaultDeleteRowsNum int64 = 100000
)

var (
	// ErrVersionNotFound means can't get the database's version
	ErrVersionNotFound = errors.New("can't get the database's version")
)

// GetCreateTableSQL returns the create table statement.
func GetCreateTableSQL(ctx context.Context, db *sql.DB, schemaName string, tableName string) (string, error) {
	/*
		show create table example result:
		mysql> SHOW CREATE TABLE `test`.`itest`;
		+-------+--------------------------------------------------------------------+
		| Table | Create Table                                                                                                                              |
		+-------+--------------------------------------------------------------------+
		| itest | CREATE TABLE `itest` (
			`id` int(11) DEFAULT NULL,
		  	`name` varchar(24) DEFAULT NULL
			) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin |
		+-------+--------------------------------------------------------------------+
	*/
	query := fmt.Sprintf("SHOW CREATE TABLE %s", TableName(schemaName, tableName))

	var tbl, createTable sql.NullString
	err := db.QueryRowContext(ctx, query).Scan(&tbl, &createTable)
	if err != nil {
		return "", errors.Trace(err)
	}
	if !tbl.Valid || !createTable.Valid {
		return "", errors.NotFoundf("table %s", tableName)
	}

	return createTable.String, nil
}

// GetParserForDB discovers ANSI_QUOTES in db's session variables and returns a proper parser
func GetParserForDB(db *sql.DB) (*parser.Parser, error) {
	mode, err := GetSQLMode(db)
	if err != nil {
		return nil, err
	}

	parser2 := parser.New()
	parser2.SetSQLMode(mode)
	return parser2, nil
}

// IsTiDB returns true if this database is tidb
func IsTiDB(ctx context.Context, db *sql.DB) (bool, error) {
	version, err := GetDBVersion(ctx, db)
	if err != nil {
		log.Error("get database's version failed", zap.Error(err))
		return false, errors.Trace(err)
	}

	return strings.Contains(strings.ToLower(version), "tidb"), nil
}

// GetDBVersion returns the database's version
func GetDBVersion(ctx context.Context, db *sql.DB) (string, error) {
	/*
		example in TiDB:
		mysql> select version();
		+--------------------------------------+
		| version()                            |
		+--------------------------------------+
		| 5.7.10-TiDB-v2.1.0-beta-173-g7e48ab1 |
		+--------------------------------------+
		example in MySQL:
		mysql> select version();
		+-----------+
		| version() |
		+-----------+
		| 5.7.21    |
		+-----------+
	*/
	query := "SELECT version()"
	result, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer result.Close()

	var version sql.NullString
	for result.Next() {
		err := result.Scan(&version)
		if err != nil {
			return "", errors.Trace(err)
		}
		break
	}

	if version.Valid {
		return version.String, nil
	}

	return "", ErrVersionNotFound
}

// TableName returns `schema`.`table`
func TableName(schema, table string) string {
	return fmt.Sprintf("`%s`.`%s`", escapeName(schema), escapeName(table))
}

// GetSQLMode returns sql_mode.
func GetSQLMode(db *sql.DB) (tmysql.SQLMode, error) {
	sqlMode, err := GetSessionVariable(db, "sql_mode")
	if err != nil {
		return tmysql.ModeNone, err
	}

	mode, err := tmysql.GetSQLMode(sqlMode)
	return mode, errors.Trace(err)
}

// GetSessionVariable gets server's session variable, although argument is *sql.DB, (session) system variables may be
// set through DSN
func GetSessionVariable(db *sql.DB, variable string) (value string, err error) {
	query := fmt.Sprintf("SHOW VARIABLES LIKE '%s'", variable)
	rows, err := db.Query(query)

	if err != nil {
		return "", errors.Trace(err)
	}
	defer rows.Close()

	// Show an example.
	/*
		mysql> SHOW VARIABLES LIKE "binlog_format";
		+---------------+-------+
		| Variable_name | Value |
		+---------------+-------+
		| binlog_format | ROW   |
		+---------------+-------+
	*/

	for rows.Next() {
		err = rows.Scan(&variable, &value)
		if err != nil {
			return "", errors.Trace(err)
		}
	}

	if rows.Err() != nil {
		return "", errors.Trace(err)
	}

	return value, nil
}

// ExecSQLWithRetry executes sql with retry
func ExecSQLWithRetry(ctx context.Context, db *sql.DB, sql string, args ...interface{}) (err error) {
	for i := 0; i < DefaultRetryTime; i++ {
		startTime := time.Now()
		_, err = db.ExecContext(ctx, sql, args...)
		takeDuration := time.Since(startTime)
		if takeDuration > SlowLogThreshold {
			log.Debug("exec sql slow", zap.String("sql", sql), zap.Reflect("args", args), zap.Duration("take", takeDuration))
		}
		if err == nil {
			return nil
		}

		if ignoreError(err) {
			log.Warn("ignore execute sql error", zap.Error(err))
			return nil
		}

		if !IsRetryableError(err) {
			return errors.Trace(err)
		}

		log.Warn("exe sql failed, will try again", zap.String("sql", sql), zap.Reflect("args", args), zap.Error(err))

		if i == DefaultRetryTime-1 {
			break
		}

		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}

	return errors.Trace(err)
}

func ignoreError(err error) bool {
	// TODO: now only ignore some ddl error, add some dml error later
	if ignoreDDLError(err) {
		return true
	}

	return false
}
func ignoreDDLError(err error) bool {
	err = errors.Cause(err)
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	errCode := errors.ErrCode(mysqlErr.Number)
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrIndexExists.Code():
		return true
	case ddl.ErrDupKeyName.Code():
		return true
	default:
		return false
	}
}

// ColumnName returns `column`
func ColumnName(column string) string {
	return fmt.Sprintf("`%s`", escapeName(column))
}

// Bucket saves the bucket information from TiDB.
type Bucket struct {
	Count      int64
	LowerBound string
	UpperBound string
}

// GetBucketsInfo SHOW STATS_BUCKETS in TiDB.
func GetBucketsInfo(ctx context.Context, db *sql.DB, schema, table string, tableInfo *model.TableInfo) (map[string][]Bucket, error) {
	/*
		example in tidb:
		mysql> SHOW STATS_BUCKETS WHERE db_name= "test" AND table_name="testa";
		+---------+------------+----------------+-------------+----------+-----------+-------+---------+---------------------+---------------------+
		| Db_name | Table_name | Partition_name | Column_name | Is_index | Bucket_id | Count | Repeats | Lower_Bound         | Upper_Bound         |
		+---------+------------+----------------+-------------+----------+-----------+-------+---------+---------------------+---------------------+
		| test    | testa      |                | PRIMARY     |        1 |         0 |    64 |       1 | 1846693550524203008 | 1846838686059069440 |
		| test    | testa      |                | PRIMARY     |        1 |         1 |   128 |       1 | 1846840885082324992 | 1847056389361369088 |
		+---------+------------+----------------+-------------+----------+-----------+-------+---------+---------------------+---------------------+
	*/
	buckets := make(map[string][]Bucket)
	query := "SHOW STATS_BUCKETS WHERE db_name= ? AND table_name= ?;"
	log.Debug("GetBucketsInfo", zap.String("sql", query), zap.String("schema", schema), zap.String("table", table))

	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	for rows.Next() {
		var dbName, tableName, partitionName, columnName, lowerBound, upperBound sql.NullString
		var isIndex, bucketID, count, repeats, ndv sql.NullInt64

		// add partiton_name in new version
		switch len(cols) {
		case 9:
			err = rows.Scan(&dbName, &tableName, &columnName, &isIndex, &bucketID, &count, &repeats, &lowerBound, &upperBound)
		case 10:
			err = rows.Scan(&dbName, &tableName, &partitionName, &columnName, &isIndex, &bucketID, &count, &repeats, &lowerBound, &upperBound)
		case 11:
			err = rows.Scan(&dbName, &tableName, &partitionName, &columnName, &isIndex, &bucketID, &count, &repeats, &lowerBound, &upperBound, &ndv)
		default:
			return nil, errors.New("Unknown struct for buckets info")
		}
		if err != nil {
			return nil, errors.Trace(err)
		}

		if _, ok := buckets[columnName.String]; !ok {
			buckets[columnName.String] = make([]Bucket, 0, 100)
		}
		buckets[columnName.String] = append(buckets[columnName.String], Bucket{
			Count:      count.Int64,
			LowerBound: lowerBound.String,
			UpperBound: upperBound.String,
		})
	}

	// when primary key is int type, the columnName will be column's name, not `PRIMARY`, check and transform here.
	indices := FindAllIndex(tableInfo)
	for _, index := range indices {
		if index.Name.O != "PRIMARY" {
			continue
		}
		_, ok := buckets[index.Name.O]
		if !ok && len(index.Columns) == 1 {
			if _, ok := buckets[index.Columns[0].Name.O]; !ok {
				return nil, errors.NotFoundf("primary key on %s in buckets info", index.Columns[0].Name.O)
			}
			buckets[index.Name.O] = buckets[index.Columns[0].Name.O]
			delete(buckets, index.Columns[0].Name.O)
		}
	}

	return buckets, errors.Trace(rows.Err())
}

// AnalyzeValuesFromBuckets analyze upperBound or lowerBound to string for each column.
// upperBound and lowerBound are looks like '(123, abc)' for multiple fields, or '123' for one field.
func AnalyzeValuesFromBuckets(valueString string, cols []*model.ColumnInfo) ([]string, error) {
	// FIXME: maybe some values contains '(', ')' or ', '
	vStr := strings.Trim(valueString, "()")
	values := strings.Split(vStr, ", ")
	if len(values) != len(cols) {
		return nil, errors.Errorf("analyze value %s failed", valueString)
	}

	for i, col := range cols {
		if IsTimeTypeAndNeedDecode(col.Tp) {
			// check if values[i] is already a time string
			sc := &stmtctx.StatementContext{TimeZone: time.UTC}
			_, err := types.ParseTime(sc, values[i], col.Tp, types.MinFsp)
			if err == nil {
				continue
			}

			value, err := DecodeTimeInBucket(values[i])
			if err != nil {
				log.Error("analyze values from buckets", zap.String("column", col.Name.O), zap.String("value", values[i]), zap.Error(err))
				return nil, errors.Trace(err)
			}

			values[i] = value
		}
	}

	return values, nil
}

// DecodeTimeInBucket decodes Time from a packed uint64 value.
func DecodeTimeInBucket(packedStr string) (string, error) {
	packed, err := strconv.ParseUint(packedStr, 10, 64)
	if err != nil {
		return "", err
	}

	if packed == 0 {
		return "", nil
	}

	t := new(types.Time)
	err = t.FromPackedUint(packed)
	if err != nil {
		return "", err
	}

	return t.String(), nil
}

// GetRandomValues returns some random value. Tips: limitArgs is the value in limitRange.
func GetRandomValues(ctx context.Context, db *sql.DB, schemaName, table, column string, num int, limitRange string, limitArgs []interface{}, collation string) ([]string, error) {
	/*
		example:
		mysql> SELECT `id` FROM (SELECT `id`, rand() rand_value FROM `test`.`test`  WHERE `id` COLLATE "latin1_bin" > 0 AND `id` COLLATE "latin1_bin" < 100 ORDER BY rand_value LIMIT 5) rand_tmp ORDER BY `id` COLLATE "latin1_bin";
		+------+
		| id   |
		+------+
		|    1 |
		|    2 |
		|    3 |
		+------+
	*/

	if limitRange == "" {
		limitRange = "TRUE"
	}

	if collation != "" {
		collation = fmt.Sprintf(" COLLATE \"%s\"", collation)
	}

	query := fmt.Sprintf("SELECT %[1]s FROM (SELECT %[1]s, rand() rand_value FROM %[2]s WHERE %[3]s ORDER BY rand_value LIMIT %[4]d)rand_tmp ORDER BY %[1]s%[5]s",
		ColumnName(column), TableName(schemaName, table), limitRange, num, collation)
	log.Debug("get random values", zap.String("sql", query), zap.Reflect("args", limitArgs))

	rows, err := db.QueryContext(ctx, query, limitArgs...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	randomValue := make([]string, 0, num)
	for rows.Next() {
		var value string
		err = rows.Scan(&value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		randomValue = append(randomValue, value)
	}

	return randomValue, errors.Trace(rows.Err())
}

// GetRowCount returns row count of the table.
// if not specify where condition, return total row count of the table.
func GetRowCount(ctx context.Context, db *sql.DB, schemaName string, tableName string, where string, args []interface{}) (int64, error) {
	/*
		select count example result:
		mysql> SELECT count(1) cnt from `test`.`itest` where id > 0;
		+------+
		| cnt  |
		+------+
		|  100 |
		+------+
	*/

	query := fmt.Sprintf("SELECT COUNT(1) cnt FROM %s", TableName(schemaName, tableName))
	if len(where) > 0 {
		query += fmt.Sprintf(" WHERE %s", where)
	}
	log.Debug("get row count", zap.String("sql", query), zap.Reflect("args", args))

	var cnt sql.NullInt64
	err := db.QueryRowContext(ctx, query, args...).Scan(&cnt)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !cnt.Valid {
		return 0, errors.NotFoundf("table `%s`.`%s`", schemaName, tableName)
	}

	return cnt.Int64, nil
}

// GetCRC32Checksum returns checksum code of some data by given condition
func GetCRC32Checksum(ctx context.Context, db *sql.DB, schemaName, tableName string, tbInfo *model.TableInfo, limitRange string, args []interface{}) (int64, error) {
	/*
		calculate CRC32 checksum example:
		mysql> SELECT BIT_XOR(CAST(CRC32(CONCAT_WS(',', id, name, age, CONCAT(ISNULL(id), ISNULL(name), ISNULL(age))))AS UNSIGNED)) AS checksum FROM test.test WHERE id > 0 AND id < 10;
		+------------+
		| checksum   |
		+------------+
		| 1466098199 |
		+------------+
	*/
	columnNames := make([]string, 0, len(tbInfo.Columns))
	columnIsNull := make([]string, 0, len(tbInfo.Columns))
	for _, col := range tbInfo.Columns {
		columnNames = append(columnNames, ColumnName(col.Name.O))
		columnIsNull = append(columnIsNull, fmt.Sprintf("ISNULL(%s)", ColumnName(col.Name.O)))
	}

	// SQL FORCE INDEX(PRIMARY)
	query := fmt.Sprintf("SELECT BIT_XOR(CAST(CRC32(CONCAT_WS(',', %s, CONCAT(%s)))AS UNSIGNED)) AS checksum FROM %s FORCE INDEX(PRIMARY) WHERE %s;",
		strings.Join(columnNames, ", "), strings.Join(columnIsNull, ", "), TableName(schemaName, tableName), limitRange)
	log.Debug("checksum", zap.String("sql", query), zap.Reflect("args", args))

	var checksum sql.NullInt64
	err := db.QueryRowContext(ctx, query, args...).Scan(&checksum)
	if err != nil {
		return -1, errors.Trace(err)
	}
	if !checksum.Valid {
		// if don't have any data, the checksum will be `NULL`
		log.Warn("get empty checksum", zap.String("sql", query), zap.Reflect("args", args))
		return 0, nil
	}

	return checksum.Int64, nil
}

// ReplacePlaceholder will use args to replace '?', used for log.
// tips: make sure the num of "?" is same with len(args)
func ReplacePlaceholder(str string, args []string) string {
	/*
		for example:
		str is "a > ? AND a < ?", args is {'1', '2'},
		this function will return "a > '1' AND a < '2'"
	*/
	newStr := strings.Replace(str, "?", "'%s'", -1)
	return fmt.Sprintf(newStr, utils.StringsToInterfaces(args)...)
}

// DeleteRows delete rows in several times. Only can delete less than 300,000 one time in TiDB.
func DeleteRows(ctx context.Context, db *sql.DB, schemaName string, tableName string, where string, args []interface{}) error {
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s limit %d;", TableName(schemaName, tableName), where, DefaultDeleteRowsNum)
	result, err := db.ExecContext(ctx, deleteSQL, args...)
	if err != nil {
		return errors.Trace(err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return errors.Trace(err)
	}

	if rows < DefaultDeleteRowsNum {
		return nil
	}

	return DeleteRows(ctx, db, schemaName, tableName, where, args)
}
