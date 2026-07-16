package com.lingyun.business.keywordSearch.html.category.job;

import com.lingyun.business.common.util.JobSourceFactory;
import com.lingyun.business.common.model.category.DWDCategoryRecord;
import com.lingyun.business.common.model.category.ODSCategoryRecord;
import com.lingyun.business.keywordSearch.html.category.process.DWDKeyCategoryProcessor;
import com.lingyun.business.keywordSearch.html.category.process.ODSKeyCategoryProcessor;
import com.lingyun.business.keywordSearch.html.category.sink.DorisBatchSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.lingyun.business.common.util.PropertiesUtil.configureCheckpoint;

/**
 * Flink 关键词搜索页类目数据写入任务
 * 默认从Kafka读取数据，--local true 时读取本地样例文件，经过ODS和DWD层处理后写入Doris
 */
public class FlinkKeyCategoryJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkKeyCategoryJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureCheckpoint(env);

        DataStream<String> sourceStream = JobSourceFactory.createSourceStream(
                env, args, "keywordSearch", "keyword-html-category",
                "keywordSearch", "samples/关键词搜索接口.json");

        SingleOutputStreamOperator<ODSCategoryRecord> odsStream = sourceStream
                .flatMap(new ODSKeyCategoryProcessor())
                .filter(record -> record != null && record.getOptId() != null && !record.getOptId().isEmpty())
                .name("ODS_Process")
                .uid("ods-process");

        odsStream
                .map(DorisBatchSink::odsKeyCategoryRecordToJson)
                .name("ODS_ToJson")
                .uid("ods-to-json")
                .filter(json -> json != null && !json.isEmpty())
                .sinkTo(DorisBatchSink.createODSDorisSink())
                .name("ODS_Sink")
                .uid("ods-sink");

        SingleOutputStreamOperator<DWDCategoryRecord> dwdStream = odsStream
                .map(new DWDKeyCategoryProcessor())
                .filter(record -> record != null && record.getOptId() != null && !record.getOptId().isEmpty())
                .name("DWD_Process")
                .uid("dwd-process");

        dwdStream
                .map(DorisBatchSink::dwdKeyCategoryRecordToJson)
                .name("DWD_ToJson")
                .uid("dwd-to-json")
                .filter(json -> json != null && !json.isEmpty())
                .sinkTo(DorisBatchSink.createDWDDorisSink())
                .name("DWD_Sink")
                .uid("dwd-sink");

        LOGGER.info("启动Flink任务: Kafka -> Doris (关键词搜索-类目)");
        env.execute("keywordSearch_html_category");
    }
}
