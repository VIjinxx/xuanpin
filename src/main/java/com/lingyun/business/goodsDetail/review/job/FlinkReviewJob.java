package com.lingyun.business.goodsDetail.review.job;

import com.lingyun.business.common.model.review.DWDReviewRecord;
import com.lingyun.business.common.model.review.ODSReviewRecord;
import com.lingyun.business.common.util.JobSourceFactory;
import com.lingyun.business.goodsDetail.review.process.DWDReviewProcessor;
import com.lingyun.business.goodsDetail.review.process.ODSReviewProcessor;
import com.lingyun.business.goodsDetail.review.sink.DorisBatchSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.lingyun.business.common.util.PropertiesUtil.configureCheckpoint;

/**
 * Flink Review 写入任务。
 * 从Kafka读取商品详情页数据，按评论列表展开后写入ODS/DWD评论表。
 */
public class FlinkReviewJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkReviewJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureCheckpoint(env);

        DataStream<String> sourceStream = JobSourceFactory.createSourceStream(
                env, args, "productDetail", "goods-detail-review",
                "productDetail", "samples/商品详情页.json");

        SingleOutputStreamOperator<ODSReviewRecord> odsStream = sourceStream
                .flatMap(new ODSReviewProcessor())
                .filter(FlinkReviewJob::keepOdsRecord)
                .name("ODS_Process")
                .uid("ods-process");

        odsStream
                .map(DorisBatchSink::odsRecordToJson)
                .name("ODS_ToJson")
                .uid("ods-to-json")
                .filter((String json) -> json != null && !json.isEmpty())
                .sinkTo(DorisBatchSink.createODSDorisSink())
                .name("ODS_Sink")
                .uid("ods-sink");

        SingleOutputStreamOperator<DWDReviewRecord> dwdStream = odsStream
                .map(new DWDReviewProcessor())
                .filter(FlinkReviewJob::keepDwdRecord)
                .name("DWD_Process")
                .uid("dwd-process");

        dwdStream
                .map(DorisBatchSink::dwdRecordToJson)
                .name("DWD_ToJson")
                .uid("dwd-to-json")
                .filter((String json) -> json != null && !json.isEmpty())
                .sinkTo(DorisBatchSink.createDWDDorisSink())
                .name("DWD_Sink")
                .uid("dwd-sink");

        LOGGER.info("启动Flink Review任务: Kafka -> Doris");
        env.execute("goodsDetail_review");
    }

    static boolean hasRequiredKeys(ODSReviewRecord record) {
        return record != null
                && hasText(record.getReviewId())
                && record.getSiteId() != null
                && hasText(record.getDate());
    }

    static boolean hasRequiredKeys(DWDReviewRecord record) {
        return record != null
                && hasText(record.getReviewId())
                && record.getSiteId() != null
                && hasText(record.getDate());
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static boolean keepOdsRecord(ODSReviewRecord record) {
        boolean keep = hasRequiredKeys(record);
        if (!keep) {
            LOGGER.warn("ODS评论记录缺少主键字段,已过滤: reviewId={}, siteId={}, date={}",
                    record == null ? null : record.getReviewId(),
                    record == null ? null : record.getSiteId(),
                    record == null ? null : record.getDate());
        }
        return keep;
    }

    private static boolean keepDwdRecord(DWDReviewRecord record) {
        boolean keep = hasRequiredKeys(record);
        if (!keep) {
            LOGGER.warn("DWD评论记录缺少主键字段,已过滤: reviewId={}, siteId={}, date={}",
                    record == null ? null : record.getReviewId(),
                    record == null ? null : record.getSiteId(),
                    record == null ? null : record.getDate());
        }
        return keep;
    }
}
