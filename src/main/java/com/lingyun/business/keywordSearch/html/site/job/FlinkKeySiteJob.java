package com.lingyun.business.keywordSearch.html.site.job;

import com.lingyun.business.common.util.JobSourceFactory;
import com.lingyun.business.common.model.site.DWDSiteRecord;
import com.lingyun.business.common.model.site.ODSSiteRecord;
import com.lingyun.business.keywordSearch.html.site.process.DWDKeySiteProcessor;
import com.lingyun.business.keywordSearch.html.site.process.ODSKeySiteProcessor;
import com.lingyun.business.keywordSearch.html.site.sink.DorisBatchSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.lingyun.business.common.util.PropertiesUtil.configureCheckpoint;

/**
 * Flink 关键词搜索页站点数据写入任务
 * 从Kafka读取数据，经过ODS和DWD层处理后写入Doris
 *
 * @author wxx
 */
public class FlinkKeySiteJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkKeySiteJob.class);

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境（并行度由提交任务时指定）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint 优化配置
        configureCheckpoint(env);

        // 2. 创建数据源：默认 Kafka；--local true 时读取本地样例文件
        DataStream<String> sourceStream = JobSourceFactory.createSourceStream(
                env, args, "keywordSearch", "keyword-html-site",
                "keywordSearch", "samples/关键词搜索接口.json");

        // 4. ODS层处理：topic → ODSSiteRecord
        SingleOutputStreamOperator<ODSSiteRecord> odsStream = sourceStream
                .map(new ODSKeySiteProcessor())
                .filter((ODSSiteRecord record) -> record != null)
                .name("ODS_Process")
                .uid("ods-process");

        // 5. ODS写入Doris
        odsStream
                .map(DorisBatchSink::odsKeySiteRecordToJson)
                .name("ODS_ToJson")
                .uid("ods-to-json")
                .filter((String json) -> json != null && !json.isEmpty())
                .sinkTo(DorisBatchSink.createODSDorisSink())
                .name("ODS_Sink")
                .uid("ods-sink");

        // 6. DWD层处理：ODSSiteRecord → DWDSiteRecord
        SingleOutputStreamOperator<DWDSiteRecord> dwdStream = odsStream
                .map(new DWDKeySiteProcessor())
                .filter((DWDSiteRecord record) -> record != null)
                .name("DWD_Process")
                .uid("dwd-process");

        // 7. DWD写入Doris
        dwdStream
                .map(DorisBatchSink::dwdKeySiteRecordToJson)
                .name("DWD_ToJson")
                .uid("dwd-to-json")
                .filter((String json) -> json != null && !json.isEmpty())
                .sinkTo(DorisBatchSink.createDWDDorisSink())
                .name("DWD_Sink")
                .uid("dwd-sink");

        LOGGER.info("启动Flink任务: Kafka -> Doris (关键词搜索-站点)");
        env.execute("keywordSearch_html_site");
    }
}
