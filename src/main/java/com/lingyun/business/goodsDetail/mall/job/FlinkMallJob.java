package com.lingyun.business.goodsDetail.mall.job;

import com.lingyun.business.common.model.mall.DWDMallsRecord;
import com.lingyun.business.common.model.mall.ODSMallsRecord;
import com.lingyun.business.common.util.JobSourceFactory;
import com.lingyun.business.goodsDetail.mall.process.DWDMallsProcessor;
import com.lingyun.business.goodsDetail.mall.process.ODSMallsProcessor;
import com.lingyun.business.goodsDetail.mall.sink.DorisBatchSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.lingyun.business.common.util.PropertiesUtil.configureCheckpoint;

/**
 * Flink Mall 写入任务
 * 从Kafka读取数据，经过ODS和DWD层处理后写入Doris
 *
 * @author wxx
 */
public class FlinkMallJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkMallJob.class);

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境（并行度由提交任务时指定）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint 优化配置
        configureCheckpoint(env);

        // 2. 创建数据源：默认 Kafka；--local true 时读取本地样例文件
        DataStream<String> sourceStream = JobSourceFactory.createSourceStream(
                env, args, "productDetail", "goods-detail-mall",
                "productDetail", "samples/商品详情页.json");

        // 4. ODS层处理：topic → ODSMallsRecord
        SingleOutputStreamOperator<ODSMallsRecord> odsStream = sourceStream
                .map(new ODSMallsProcessor())
                .filter((ODSMallsRecord record) -> record != null)
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

        // 6. DWD层处理：ODSMallsRecord → DWDMallsRecord
        SingleOutputStreamOperator<DWDMallsRecord> dwdStream = odsStream
                .map(new DWDMallsProcessor())
                .filter((DWDMallsRecord record) -> record != null)
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

        LOGGER.info("启动Flink任务: Kafka -> Doris");
        env.execute("goodsDetail_mall");
    }
}
