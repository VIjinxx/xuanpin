package com.lingyun.business.goodsDetail.goods.job;

import com.lingyun.business.common.model.goods.DWDGoodsRecord;
import com.lingyun.business.common.model.goods.ODSGoodsRecord;
import com.lingyun.business.common.util.JobSourceFactory;
import com.lingyun.business.goodsDetail.goods.process.DWDGoodsProcessor;
import com.lingyun.business.goodsDetail.goods.process.ODSGoodsProcessor;
import com.lingyun.business.goodsDetail.goods.sink.DorisBatchSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.lingyun.business.common.util.PropertiesUtil.configureCheckpoint;

/**
 * Flink Goods 写入任务
 * 从Kafka读取商品详情页数据,经过ODS和DWD层处理后写入Doris
 *
 * @author wxx
 */
public class FlinkGoodsJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkGoodsJob.class);

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境(并行度由提交任务时指定)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint 优化配置
        configureCheckpoint(env);

        // 2. 创建数据源：默认 Kafka；--local true 时读取本地样例文件
        DataStream<String> sourceStream = JobSourceFactory.createSourceStream(
                env, args, "productDetail", "goods-detail-goods",
                "productDetail", "samples/商品详情页.json");

        // 4. ODS层处理: topic → ODSGoodsRecord
        SingleOutputStreamOperator<ODSGoodsRecord> odsStream = sourceStream
                .flatMap(new ODSGoodsProcessor())
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

        // 6. DWD层处理: ODSGoodsRecord → DWDGoodsRecord
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

        LOGGER.info("启动Flink任务: Kafka -> Doris");
        env.execute("goodsDetail_goods");
    }
}
