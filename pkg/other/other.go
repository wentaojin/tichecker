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
package other

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// 判断字符是否是数字
func IsNum(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

// 字符串切分
func StringSplit(s string) []string {
	return strings.Split(s, ".")
}

/**
* function 判断文件/文件夹是否存在
* param   path: 文件/文件夹的路径
* return  bool：true存在，false不存在
 */
func FileAndDirIsExist(path string) bool {
	_, erByStat := os.Stat(path)
	if erByStat != nil {
		//该判断主要是部分文件权限问题导致os.Stat()出错,具体看业务启用
		//使用os.IsNotExist()判断为true,说明文件或文件夹不存在
		if os.IsNotExist(erByStat) {
			return false
		} else {
			return true
		}
	}
	return true
}

// 创建文件夹
func TouchPath(path string, dirPerm os.FileMode) error {
	_, err := os.Stat(path)
	if err == nil {
		t := time.Now().Local()
		err = os.Chtimes(path, t, t)
		return err
	}

	// Create directory path
	dir := filepath.Dir(path)
	_, err = os.Stat(dir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dir, dirPerm)
	}
	if err != nil {
		return err
	}

	return err
}

// 移除文件夹
func RemovePath(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove [%v] dir failed: %v", path, err)
	}
	return nil
}
