package com.lingyun.business.goodsDetail.site.job;

import com.lingyun.business.common.util.JobSourceFactory;
import com.lingyun.business.common.model.site.DWDSiteRecord;
import com.lingyun.business.common.model.site.ODSSiteRecord;
import com.lingyun.business.goodsDetail.site.process.DWDSiteProcessor;
import com.lingyun.business.goodsDetail.site.process.ODSSiteProcessor;
import com.lingyun.business.goodsDetail.site.sink.DorisBatchSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.lingyun.business.common.util.PropertiesUtil.configureCheckpoint;

/**
 * Flink Site 写入任务
 * 从Kafka读取数据，经过ODS和DWD层处理后写入Doris
 *
 * @author wxx
 */
public class FlinkSiteJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkSiteJob.class);

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境（并行度由提交任务时指定）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint 优化配置
        configureCheckpoint(env);

        // 2. 创建数据源：默认 Kafka；--local true 时读取本地样例文件
        DataStream<String> sourceStream = JobSourceFactory.createSourceStream(
                env, args, "productDetail", "goods-detail-site",
                "productDetail", "samples/商品详情页.json");

        // 4. ODS层处理：topic → ODSSiteRecord
        SingleOutputStreamOperator<ODSSiteRecord> odsStream = sourceStream
                .map(new ODSSiteProcessor())
                .filter((ODSSiteRecord record) -> record != null)
                .name("ODS_Process")
                .uid("ods-process");

        // 5. ODS写入Doris
        odsStream
                .map(DorisBatchSink::odsSiteRecordToJson)
                .name("ODS_ToJson")
                .uid("ods-to-json")
                .filter((String json) -> json != null && !json.isEmpty())
                .sinkTo(DorisBatchSink.createODSDorisSink())
                .name("ODS_Sink")
                .uid("ods-sink");

        // 6. DWD层处理：ODSSiteRecord → DWDSiteRecord
        SingleOutputStreamOperator<DWDSiteRecord> dwdStream = odsStream
                .map(new DWDSiteProcessor())
                .filter((DWDSiteRecord record) -> record != null)
                .name("DWD_Process")
                .uid("dwd-process");

        // 7. DWD写入Doris
        dwdStream
                .map(DorisBatchSink::dwdSiteRecordToJson)
                .name("DWD_ToJson")
                .uid("dwd-to-json")
                .filter((String json) -> json != null && !json.isEmpty())
                .sinkTo(DorisBatchSink.createDWDDorisSink())
                .name("DWD_Sink")
                .uid("dwd-sink");

        LOGGER.info("启动Flink任务: Kafka -> Doris");
        env.execute("goodsDetail_site");
    }
}

