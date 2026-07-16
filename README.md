# lingfeng_xuanpin_flink_task

多个灵枫数据领域的 Kafka -> Flink -> Doris 作业。该仓库还包括一个独立的 Kafka 压力测试工具用于负载生成。

## 要求

- JDK 8
- Maven 3.6+
- 访问在 `src/main/resources/application.properties` 中配置的 Kafka 和 Doris 端点

## 构建一个完整的 jar 包

项目打包一个包含所有可运行任务及其依赖的 fat jar：

```bash
mvn package
```

主要输出：

- `target/lingfeng_xuanpin_flink_task-1.0-SNAPSHOT-all.jar`

## 外部数据源 JSONL 导入 Kafka

外部数据源任务（如 `geekbi-goods`）的离线 JSONL 数据，可在 Windows 本地通过 Python 脚本写入 Kafka，再由 Flink 任务消费。

安装依赖：

```bash
pip install -r scripts/requirements.txt
```

全量导入：

```powershell
python scripts/file_to_kafka.py `
  --file "<jsonl-file-path>" `
  --topic geekbiGoods `
  --bootstrap-servers hdp01:9092,hdp02:9092,hdp03:9092 `
  --key-by-goods-id `
  --checkpoint-file "<checkpoint-file-path>" `
  --checkpoint-interval 10000 `
  --resume
```

中断后再次执行相同命令即可从 checkpoint 续传。导入完成后在服务器执行 `./wstart geekbi-goods` 启动 Flink 任务。

## 任务脚本

部署脚本现在只管理 Flink 作业。操作员使用任务名称而不是编写完整的 `flink run` 命令：

```bash
./bin/wstart list
./bin/wstart status
./bin/wstart store-detail-mall
./bin/wstart -jar lingfeng_xuanpin_flink_task-1.0-SNAPSHOT-all.jar store-detail-mall
./bin/wstart all
./bin/wstop list
./bin/wstop store-detail-mall
./bin/wstop all
```

## 服务器布局

使用以下结构部署：

```text
deploy/
|-- bin/
|   |-- wstart
|   `-- wstop
`-- jar/
    |-- kafkaTest.sh
    `-- lingfeng_xuanpin_flink_task-1.0-SNAPSHOT-all.jar
```

将仓库中的 `scripts/wstart` 和 `scripts/wstop` 复制到服务器的 `bin/` 目录。将 `scripts/kafkaTest.sh` 复制到与打包好的 jar 相同的目录。默认情况下，启动脚本查找 `../jar/lingfeng_xuanpin*.jar`。如果存在多个匹配的 jar 文件，请使用 `-jar <jar-file>` 明确选择一个。

启动脚本只使用前导脚本参数，如 `-jar <jar-file>` 和 `<task-name|all|list|status>`。`all` 启动每个任务，`list` 打印支持的任务名称，`status` 打印当前的 YARN 状态，任何其他目标都被视为任务名称。额外的任务参数仅支持单任务启动。

对于 Flink 作业，启动脚本包装一个相当于以下命令的命令：

```bash
flink run \
  -t yarn-per-job \
  -D yarn.application.name=lingfeng-<task-name> \
  -d \
  -D jobmanager.memory.process.size=1024m \
  -D taskmanager.memory.process.size=<taskmanager-memory> \
  -D taskmanager.numberOfTaskSlots=<task-slots> \
  -D classloader.check-leaked-classloader=false \
  -D log4j.configurationFile=file:/opt/module/flink/conf/log4j.properties \
  -p <parallelism> \
  -c <main-class> \
  ../jar/lingfeng_xuanpin*.jar
```

TaskManager 内存、任务槽和并行度在 `wstart` 文件顶部的 `FLINK_JOB_RESOURCES` 表中按任务定义。JobManager 内存由所有作业共享，默认为 `1024m`。

停止脚本匹配此行为：

- Flink 作业通过 YARN 应用程序名称停止
- `all` 停止内置任务列表中的每个任务

当您启动单个任务或运行 `wstart all` 时，脚本首先检查相同的 YARN 应用程序名称是否已在运行。如果是，脚本停止旧任务，等待几秒钟，然后重新启动它。

当 `wstart all` 完成时，启动脚本再次查询 YARN 并打印最终任务状态块，例如：

```text
已启动任务：
task-a
task-b

未启动任务：
task-c
```

停止脚本仍然打印摘要日志，例如：

- `已停止任务：...`
- `未运行任务：...`
- `停止失败任务：...`

`wstart status` 查询一次 YARN 并打印：

- `已启动任务：` 绿色显示
- 已启动的任务名称，每行一个，黄色显示
- `未启动任务：` 绿色显示
- 未启动的任务名称，每行一个，红色显示

## 支持的任务名称

| 任务名称 | 主类 |
| --- | --- |
| `store-detail-mall` | `com.lingyun.business.storeDetail.mall.job.FlinkMallJob` |
| `goods-detail-category` | `com.lingyun.business.goodsDetail.category.job.FlinkCategoryJob` |
| `goods-detail-goods` | `com.lingyun.business.goodsDetail.goods.job.FlinkGoodsJob` |
| `goods-detail-mall` | `com.lingyun.business.goodsDetail.mall.job.FlinkMallJob` |
| `goods-detail-review` | `com.lingyun.business.goodsDetail.review.job.FlinkReviewJob` |
| `goods-detail-site` | `com.lingyun.business.goodsDetail.site.job.FlinkSiteJob` |
| `keyword-html-category` | `com.lingyun.business.keywordSearch.html.category.job.FlinkKeyCategoryJob` |
| `keyword-html-goods` | `com.lingyun.business.keywordSearch.html.goods.job.FlinkKeyGoodsJob` |
| `keyword-html-site` | `com.lingyun.business.keywordSearch.html.site.job.FlinkKeySiteJob` |
| `keyword-more-category` | `com.lingyun.business.keywordSearch.more.category.job.FlinkKeyMoreCategoryJob` |
| `keyword-more-goods` | `com.lingyun.business.keywordSearch.more.goods.job.FlinkKeyMoreGoodsJob` |

## 运行时注意事项

- Kafka 引导服务器、消费者组、Doris 端点和检查点设置从 `src/main/resources/application.properties` 加载。
- Flink 作业类通过 `FlinkKafkaDao` 在代码中覆盖 Kafka 主题。
- Flink 提交参数可以通过环境变量覆盖，如 `FLINK_BIN`、`FLINK_APP_NAME`、`FLINK_JOBMANAGER_MEMORY`、`FLINK_TASKMANAGER_MEMORY`、`FLINK_TASK_SLOTS`、`FLINK_PARALLELISM`、`FLINK_EXTRA_OPTS`、`RESTART_WAIT_SECONDS`、`YARN_START_WAIT_SECONDS`、`YARN_STOP_WAIT_SECONDS` 和 `STOP_WAIT_SECONDS`。

## 本地样本测试

这些作业参数适用于本地测试，不适用于部署脚本：

```bash
flink run -c <main-class> target/lingfeng_xuanpin_flink_task-1.0-SNAPSHOT-all.jar --local true
flink run -c <main-class> target/lingfeng_xuanpin_flink_task-1.0-SNAPSHOT-all.jar --local true --file samples/商品详情页.json
flink run -c <main-class> target/lingfeng_xuanpin_flink_task-1.0-SNAPSHOT-all.jar --local true --file /path/to/sample.json
```

没有 `--local true` 时，作业从 Kafka 读取。有 `--local true` 时，每个作业读取 `src/main/resources/samples/` 下的默认样本；`--file` 覆盖该样本路径。首先尝试文件系统路径，然后是类路径资源。
