package com.lingyun.business.keywordSearch.html.goods.job;

import com.lingyun.business.common.util.JobSourceFactory;
import com.lingyun.business.common.model.goods.DWDGoodsRecord;
import com.lingyun.business.common.model.goods.ODSGoodsRecord;
import com.lingyun.business.goodsDetail.goods.process.DWDGoodsProcessor;
import com.lingyun.business.keywordSearch.html.goods.process.ODSKeyGoodsProcessor;
import com.lingyun.business.keywordSearch.html.goods.sink.DorisBatchSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.lingyun.business.common.util.PropertiesUtil.configureCheckpoint;

/**
 * Flink 关键词搜索页商品数据写入任务
 * 从Kafka读取数据，经过ODS和DWD层处理后写入Doris
 *
 * @author wxx
 */
public class FlinkKeyGoodsJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkKeyGoodsJob.class);

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境（并行度由提交任务时指定）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint 优化配置
        configureCheckpoint(env);

        // 2. 创建数据源：默认 Kafka；--local true 时读取本地样例文件
        DataStream<String> sourceStream = JobSourceFactory.createSourceStream(
                env, args, "keywordSearch", "keyword-html-goods",
                "keywordSearch", "samples/关键词搜索接口.json");

        // 4. ODS层处理：topic → ODSGoodsRecord
        SingleOutputStreamOperator<ODSGoodsRecord> odsStream = sourceStream
                .flatMap(new ODSKeyGoodsProcessor())
                .filter((ODSGoodsRecord record) -> record != null)
                .name("ODS_Process")
                .uid("ods-process");

        // 5. ODS写入Doris
        odsStream
                .map(DorisBatchSink::odsRecordToJson)
                .name("ODS_ToJson")
                .uid("ods-to-json")
                .filter((String json) -> json != null && !json.isEmpty())
                .sinkTo(DorisBatchSink.createODSDorisSink())
                .name("ODS_Sink")
                .uid("ods-sink");

        // 6. DWD层处理：ODSGoodsRecord → DWDGoodsRecord
        SingleOutputStreamOperator<DWDGoodsRecord> dwdStream = odsStream
                .map(new DWDGoodsProcessor())
                .filter((DWDGoodsRecord record) -> record != null)
                .name("DWD_Process")
                .uid("dwd-process");

        // 7. DWD写入Doris
        dwdStream
                .map(DorisBatchSink::dwdRecordToJson)
                .name("DWD_ToJson")
                .uid("dwd-to-json")
                .filter((String json) -> json != null && !json.isEmpty())
                .sinkTo(DorisBatchSink.createDWDDorisSink())
                .name("DWD_Sink")
                .uid("dwd-sink");

        LOGGER.info("启动Flink任务: Kafka -> Doris (关键词搜索-商品)");
        env.execute("keywordSearch_html_goods");
    }
}
