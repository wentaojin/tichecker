# tichecker

ticheker 工具用于在线 Online 校验 MySQL -> TiDB 基于 [DM](https://docs.pingcap.com/zh/tidb-data-migration/stable) 实时同步数据一致性，支持合库合表场景，秉承最小业务侵入原则，非 binlog 以及加事务锁机制，主要原理是基于上下游表范围 chunk 计算 checksum【chunk 切分尽可能小，按理数据变动可能性更小，可能数据核对会更准，比对出不一致数据更快，但是检验时间会更长】，如果 checksum 校验出现数据不一致则会按照配置项进行重试，重试指定次数仍然不一致则会记录并产生修复 SQL，使用限制：

- 有损 DDL 变更期间无法使用，比如：rename column/drop column/ 上下游表结构不一致情况下
- 即使产生数据不一致，也可能是因 DM 同步延迟超时重试次数所致，需要手工再次核对确认或者调大重试次数、重试等待间隔时间重新校验

使用方式：

- 详情配置文件示例见 conf/tichecker.toml

```
$ ./tichecker --config tichecker.toml
```
