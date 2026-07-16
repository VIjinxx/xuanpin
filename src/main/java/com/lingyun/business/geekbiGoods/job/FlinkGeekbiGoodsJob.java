package com.lingyun.business.geekbiGoods.job;

import com.lingyun.business.common.util.JobSourceFactory;
import com.lingyun.business.geekbiGoods.model.DWDGeekbiGoodsRecord;
import com.lingyun.business.geekbiGoods.model.ODSGeekbiGoodsRecord;
import com.lingyun.business.geekbiGoods.process.DWDGeekbiGoodsProcessor;
import com.lingyun.business.geekbiGoods.process.ODSGeekbiGoodsProcessor;
import com.lingyun.business.geekbiGoods.sink.DorisBatchSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.lingyun.business.common.util.PropertiesUtil.configureCheckpoint;

/**
 * geekbiGoods Kafka Topic 到外部数据源商品 ODS/DWD 表的 Flink 任务。
 */
public class FlinkGeekbiGoodsJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkGeekbiGoodsJob.class);

    public static final String SOURCE_TOPIC = "geekbiGoods";
    public static final String CONSUMER_GROUP = "geekbi-goods";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureCheckpoint(env);

        DataStream<String> sourceStream = JobSourceFactory.createSourceStream(
                env, args, SOURCE_TOPIC, CONSUMER_GROUP,
                SOURCE_TOPIC, "samples/geekbiGoods.json");

        SingleOutputStreamOperator<ODSGeekbiGoodsRecord> odsStream = sourceStream
                .map(new ODSGeekbiGoodsProcessor())
                .filter(FlinkGeekbiGoodsJob::keepOdsRecord)
                .name("GeekbiGoods_ODS_Process")
                .uid("geekbi-goods-ods-process");

        odsStream
                .map(DorisBatchSink::odsRecordToJson)
                .filter(FlinkGeekbiGoodsJob::hasText)
                .sinkTo(DorisBatchSink.createODSDorisSink())
                .name("GeekbiGoods_ODS_Sink")
                .uid("geekbi-goods-ods-sink");

        SingleOutputStreamOperator<DWDGeekbiGoodsRecord> dwdStream = odsStream
                .flatMap(new DWDGeekbiGoodsProcessor())
                .filter(FlinkGeekbiGoodsJob::keepDwdRecord)
                .name("GeekbiGoods_DWD_Process")
                .uid("geekbi-goods-dwd-process");

        dwdStream
                .map(DorisBatchSink::dwdRecordToJson)
                .filter(FlinkGeekbiGoodsJob::hasText)
                .sinkTo(DorisBatchSink.createDWDDorisSink())
                .name("GeekbiGoods_DWD_Sink")
                .uid("geekbi-goods-dwd-sink");

        LOGGER.info("启动外部数据源商品任务: Kafka topic={} -> ODS/DWD Doris", SOURCE_TOPIC);
        env.execute("geekbiGoods");
    }

    static boolean hasRequiredKeys(ODSGeekbiGoodsRecord record) {
        return record != null
                && record.getGoodsId() != null
                && record.getSiteId() != null
                && hasText(record.getDate());
    }

    static boolean hasRequiredKeys(DWDGeekbiGoodsRecord record) {
        return record != null
                && record.getGoodsId() != null
                && record.getSiteId() != null
                && hasText(record.getDate());
    }

    private static boolean keepOdsRecord(ODSGeekbiGoodsRecord record) {
        boolean keep = hasRequiredKeys(record);
        if (!keep) {
            LOGGER.warn("外部数据源商品 ODS 记录缺少主键字段,已过滤: goodsId={}, siteId={}, date={}",
                    record == null ? null : record.getGoodsId(),
                    record == null ? null : record.getSiteId(),
                    record == null ? null : record.getDate());
        }
        return keep;
    }

    private static boolean keepDwdRecord(DWDGeekbiGoodsRecord record) {
        boolean keep = hasRequiredKeys(record);
        if (!keep) {
            LOGGER.warn("外部数据源商品 DWD 记录缺少主键字段,已过滤: goodsId={}, siteId={}, date={}",
                    record == null ? null : record.getGoodsId(),
                    record == null ? null : record.getSiteId(),
                    record == null ? null : record.getDate());
        }
        return keep;
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
