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
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	dsn1 := "root:marvin@tcp(172.16.4.206:3307)/?charset=utf8mb4"
	dsn2 := "root:marvin@tcp(172.16.4.206:3308)/?charset=utf8mb4"

	engine1, err := newMySQLEngine(dsn1)
	if err != nil {
		fmt.Println(err)
	}
	engine2, err := newMySQLEngine(dsn2)
	if err != nil {
		fmt.Println(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	// 分片 1
	go func() {
		defer wg.Done()
		for i := 15091; i <= 50000; i++ {
			querySQL := fmt.Sprintf(`INSERT INTO dm_marvin.t_marvin2 (id,name) VALUES (%d,'Marvin')`, i)
			if err := sqlRun(engine1, querySQL); err != nil {
				fmt.Println(err)
			}

		}
	}()

	// 分片 2
	go func() {
		defer wg.Done()
		for i := 10001; i <= 50000; i++ {
			querySQL := fmt.Sprintf(`INSERT INTO dm_marvin.t_marvin1 (id,name) VALUES (%d,'Gyq')`, i)
			if err := sqlRun(engine2, querySQL); err != nil {
				fmt.Println(err)
			}
		}
	}()
	wg.Wait()
}

func newMySQLEngine(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return db, fmt.Errorf("error on initializing mysql database connection [no-schema]: %v", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	return db, nil
}

func sqlRun(engine *sql.DB, querySQL string) error {
	if _, err := engine.Exec(querySQL); err != nil {
		return err
	}
	return nil
}
