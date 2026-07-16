package com.lingyun.business.geekbiGoods.sink;

import com.lingyun.business.geekbiGoods.model.DWDGeekbiGoodsRecord;
import com.lingyun.business.geekbiGoods.model.ODSGeekbiGoodsRecord;
import org.apache.doris.flink.sink.DorisSink;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * 外部数据源商品 Doris Sink 适配器。
 */
public final class DorisBatchSink {
    public static final String ODS_TABLE = "ods_geekbi_goods_raw";
    public static final String DWD_TABLE = "dwd_geekbi_goods_daily";

    private static final AtomicLong ODS_COUNTER = new AtomicLong(0);
    private static final AtomicLong DWD_COUNTER = new AtomicLong(0);

    private DorisBatchSink() {
    }

    public static DorisSink<String> createODSDorisSink() {
        return com.lingyun.business.common.sink.DorisBatchSink.createODSDorisSink(ODS_TABLE);
    }

    public static DorisSink<String> createDWDDorisSink() {
        return com.lingyun.business.common.sink.DorisBatchSink.createDWDDorisSink(DWD_TABLE);
    }

    public static String odsRecordToJson(ODSGeekbiGoodsRecord record) {
        Function<ODSGeekbiGoodsRecord, String> logExtractor =
                item -> String.format("商品ID=%s, 站点ID=%s, 采集日期=%s",
                        item.getGoodsId(), item.getSiteId(), item.getDate());
        return com.lingyun.business.common.sink.DorisBatchSink.recordToJsonByReflection(
                record, ODSGeekbiGoodsRecord.class, ODS_COUNTER, logExtractor);
    }

    public static String dwdRecordToJson(DWDGeekbiGoodsRecord record) {
        Function<DWDGeekbiGoodsRecord, String> logExtractor =
                item -> String.format("商品ID=%s, 站点ID=%s, 统计日期=%s",
                        item.getGoodsId(), item.getSiteId(), item.getDate());
        return com.lingyun.business.common.sink.DorisBatchSink.recordToJsonByReflection(
                record, DWDGeekbiGoodsRecord.class, DWD_COUNTER, logExtractor);
    }
}
