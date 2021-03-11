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
	"sort"

	"github.com/pingcap/parser/model"
)

// FindAllIndex returns all index, order is pk, uk, and normal index.
func FindAllIndex(tableInfo *model.TableInfo) []*model.IndexInfo {
	indices := make([]*model.IndexInfo, len(tableInfo.Indices))
	copy(indices, tableInfo.Indices)
	sort.SliceStable(indices, func(i, j int) bool {
		a := indices[i]
		b := indices[j]
		switch {
		case b.Primary:
			return false
		case a.Primary:
			return true
		case b.Unique:
			return false
		case a.Unique:
			return true
		default:
			return false
		}
	})
	return indices
}

// SelectUniqueOrderKey returns some columns for order by condition.
func SelectUniqueOrderKey(tbInfo *model.TableInfo) ([]string, []*model.ColumnInfo) {
	keys := make([]string, 0, 2)
	keyCols := make([]*model.ColumnInfo, 0, 2)

	for _, index := range tbInfo.Indices {
		if index.Primary {
			keys = keys[:0]
			keyCols = keyCols[:0]
			for _, indexCol := range index.Columns {
				keys = append(keys, indexCol.Name.O)
				keyCols = append(keyCols, tbInfo.Columns[indexCol.Offset])
			}
			break
		}
		if index.Unique {
			keys = keys[:0]
			keyCols = keyCols[:0]
			for _, indexCol := range index.Columns {
				keys = append(keys, indexCol.Name.O)
				keyCols = append(keyCols, tbInfo.Columns[indexCol.Offset])
			}
		}
	}

	if len(keys) != 0 {
		return keys, keyCols
	}

	// no primary key or unique found, use all fields as order by key
	for _, col := range tbInfo.Columns {
		keys = append(keys, col.Name.O)
		keyCols = append(keyCols, col)
	}

	return keys, keyCols
}
