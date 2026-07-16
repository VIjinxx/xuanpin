package com.lingyun.business.keywordSearch.html.category.sink;

import com.lingyun.business.common.model.category.DWDCategoryRecord;
import com.lingyun.business.common.model.category.ODSCategoryRecord;
import org.apache.doris.flink.sink.DorisSink;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Doris批量写入Sink - 关键词搜索页类目适配器
 * 提供类目特定的方法，内部使用common包中的通用实现
 */
public class DorisBatchSink {

    private static final AtomicLong odsCounter = new AtomicLong(0);
    private static final AtomicLong dwdCounter = new AtomicLong(0);

    private static final String ODS_CATEGORY_TABLE = "ods_category_raw";
    private static final String DWD_CATEGORY_TABLE = "dwd_category_base";

    public static DorisSink<String> createODSDorisSink() {
        return com.lingyun.business.common.sink.DorisBatchSink.createODSDorisSink(ODS_CATEGORY_TABLE);
    }

    public static DorisSink<String> createDWDDorisSink() {
        return com.lingyun.business.common.sink.DorisBatchSink.createDWDDorisSink(DWD_CATEGORY_TABLE);
    }

    public static String odsKeyCategoryRecordToJson(ODSCategoryRecord record) {
        Function<ODSCategoryRecord, String> logExtractor = r ->
                String.format("类目ID=%s, 站点ID=%s, 类目标题=%s",
                        r.getOptId(), r.getSiteId(), r.getTitle());
        String json = com.lingyun.business.common.sink.DorisBatchSink.recordToJson(
                record, ODSCategoryRecord.class, odsCounter, logExtractor);
        return json;
    }

    public static String dwdKeyCategoryRecordToJson(DWDCategoryRecord record) {
        Function<DWDCategoryRecord, String> logExtractor = r ->
                String.format("类目ID=%s, 站点ID=%s, 类目标题=%s",
                        r.getOptId(), r.getSiteId(), r.getTitle());
        String json = com.lingyun.business.common.sink.DorisBatchSink.recordToJsonByReflection(
                record, DWDCategoryRecord.class, dwdCounter, logExtractor);
        return json;
    }
}
