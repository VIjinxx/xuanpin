package com.lingyun.business.goodsDetail.category.sink;

import com.lingyun.business.common.model.category.DWDCategoryRecord;
import com.lingyun.business.common.model.category.ODSCategoryRecord;
import org.apache.doris.flink.sink.DorisSink;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Doris批量写入Sink - Category适配器
 * 提供category特定的方法,内部使用common包中的通用实现
 *
 * @author wxx
 */
public class DorisBatchSink {

    /** 采样计数器,用于采样日志 */
    private static final AtomicLong odsCounter = new AtomicLong(0);
    private static final AtomicLong dwdCounter = new AtomicLong(0);

    // 表名常量
    private static final String ODS_CATEGORY_TABLE = "ods_category_raw";
    private static final String DWD_CATEGORY_TABLE = "dwd_category_base";

    /**
     * 创建ODS层Doris Sink
     */
    public static DorisSink<String> createODSDorisSink() {
        return com.lingyun.business.common.sink.DorisBatchSink.createODSDorisSink(ODS_CATEGORY_TABLE);
    }

    /**
     * 创建DWD层Doris Sink
     */
    public static DorisSink<String> createDWDDorisSink() {
        return com.lingyun.business.common.sink.DorisBatchSink.createDWDDorisSink(DWD_CATEGORY_TABLE);
    }

    /**
     * 将ODSCategoryRecord转换为JSON字符串
     */
    public static String odsRecordToJson(ODSCategoryRecord record) {
        Function<ODSCategoryRecord, String> logExtractor = r ->
                String.format("类目ID=%s, 站点ID=%s, 类目名=%s",
                        r.getOptId(), r.getSiteId(), r.getOptName());
        String json = com.lingyun.business.common.sink.DorisBatchSink.recordToJson(
                record, ODSCategoryRecord.class, odsCounter, logExtractor);
        return json;
    }

    /**
     * 将DWDCategoryRecord转换为JSON字符串
     */
    public static String dwdRecordToJson(DWDCategoryRecord record) {
        Function<DWDCategoryRecord, String> logExtractor = r ->
                String.format("类目ID=%s, 站点ID=%s, 类目名=%s",
                        r.getOptId(), r.getSiteId(), r.getOptName());
        String json = com.lingyun.business.common.sink.DorisBatchSink.recordToJsonByReflection(
                record, DWDCategoryRecord.class, dwdCounter, logExtractor);
        return json;
    }
}
