# tichecker

ticheker 工具用于在线 Online 校验 MySQL -> TiDB 基于 [DM](https://docs.pingcap.com/zh/tidb-data-migration/stable) 实时同步数据一致性，支持合库合表以及单库场景，主要原理是无锁基于上下游表某字段 Rang 范围切分查询检查【Rang 切分尽可能小，案例数据变动可能性更小，可能数据核对会更准，但是检验时间会更长】，如果校验出现数据不一致则会按照配置项进行重试，重试指定次数仍然不一致则会记录并产生修复 SQL，使用限制：

- DDL 变更或灰度期间无法使用
- 即使产生数据不一致，也可能是因 DM 同步延迟超时重试次数所致，需要手工再次核对确认或者调大重试次数、重试等待间隔时间重新校验

使用方式：详情配置文件示例见 conf/tichecker.toml

- 手工配置在线检验模板并统一放于 config-template-dir 目录，详情模板文件示例见 conf/t_marvin_diff.tmpl，模板文件中除下{{}}内容之外，其他都需要[手工配置](https://docs.pingcap.com/zh/tidb/stable/sync-diff-inspector-overview#sync-diff-inspector-%E7%94%A8%E6%88%B7%E6%96%87%E6%A1%A3)，并且每张表需要一个模板文件，模板文件命名格式：tableName_diff.tmpl
- 手工配置 tichecker 配置文件 conf/tichecker.toml

```
$ ./tichecker --config tichecker.toml
```

