package com.lingyun.business.keywordSearch.more.category.job;

import com.lingyun.business.common.model.category.DWDCategoryRecord;
import com.lingyun.business.common.model.category.ODSCategoryRecord;
import com.lingyun.business.common.util.JobSourceFactory;
import com.lingyun.business.goodsDetail.category.process.DWDCategoryProcessor;
import com.lingyun.business.keywordSearch.more.category.process.ODSKeyMoreCategoryProcessor;
import com.lingyun.business.keywordSearch.more.category.sink.DorisBatchSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.lingyun.business.common.util.PropertiesUtil.configureCheckpoint;

/**
 * Flink 关键词搜索查看更多分类数据写入任务
 * 默认从Kafka读取数据，--local true 时读取本地样例文件，经过ODS和DWD层处理后写入Doris
 *
 * @author wxx
 */
public class FlinkKeyMoreCategoryJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkKeyMoreCategoryJob.class);

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境（并行度由提交任务时指定）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint 优化配置
        configureCheckpoint(env);

        // 2. 创建数据源：默认 Kafka；--local true 时读取本地样例文件
        DataStream<String> sourceStream = JobSourceFactory.createSourceStream(
                env, args, "keywordSearchMore", "keyword-more-category",
                "keywordSearchMore", "samples/关键词搜索查看更多.json");

        // 3. ODS层处理：topic → ODSCategoryRecord
        SingleOutputStreamOperator<ODSCategoryRecord> odsStream = sourceStream
                .flatMap(new ODSKeyMoreCategoryProcessor())
                .filter((ODSCategoryRecord record) -> record != null
                        && record.getOptId() != null && !record.getOptId().isEmpty())
                .name("ODS_Process")
                .uid("ods-process");

        // 4. ODS写入Doris
        odsStream
                .map(DorisBatchSink::odsRecordToJson)
                .name("ODS_ToJson")
                .uid("ods-to-json")
                .filter((String json) -> json != null && !json.isEmpty())
                .sinkTo(DorisBatchSink.createODSDorisSink())
                .name("ODS_Sink")
                .uid("ods-sink");

        // 5. DWD层处理：ODSCategoryRecord → DWDCategoryRecord
        SingleOutputStreamOperator<DWDCategoryRecord> dwdStream = odsStream
                .map(new DWDCategoryProcessor())
                .filter((DWDCategoryRecord record) -> record != null
                        && record.getOptId() != null && !record.getOptId().isEmpty())
                .name("DWD_Process")
                .uid("dwd-process");

        // 6. DWD写入Doris
        dwdStream
                .map(DorisBatchSink::dwdRecordToJson)
                .name("DWD_ToJson")
                .uid("dwd-to-json")
                .filter((String json) -> json != null && !json.isEmpty())
                .sinkTo(DorisBatchSink.createDWDDorisSink())
                .name("DWD_Sink")
                .uid("dwd-sink");

        LOGGER.info("启动Flink任务: Kafka -> Doris (关键词搜索查看更多-分类)");
        env.execute("keywordSearch_more_category");
    }
}
