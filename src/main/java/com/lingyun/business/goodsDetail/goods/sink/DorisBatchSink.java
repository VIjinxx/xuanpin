package com.lingyun.business.goodsDetail.goods.sink;

import com.lingyun.business.common.model.goods.DWDGoodsRecord;
import com.lingyun.business.common.model.goods.ODSGoodsRecord;
import org.apache.doris.flink.sink.DorisSink;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Doris批量写入Sink - Goods适配器
 * 提供goods特定的方法,内部使用common包中的通用实现
 */
public class DorisBatchSink {

    /** 采样计数器,用于采样日志 */
    private static final AtomicLong odsCounter = new AtomicLong(0);
    private static final AtomicLong dwdCounter = new AtomicLong(0);

    // 表名常量
    private static final String ODS_GOODS_TABLE = "ods_goods_raw";
    private static final String DWD_GOODS_TABLE = "dwd_goods_base";

    /**
     * 创建ODS层Doris Sink
     */
    public static DorisSink<String> createODSDorisSink() {
        return com.lingyun.business.common.sink.DorisBatchSink.createODSDorisSink(ODS_GOODS_TABLE);
    }

    /**
     * 创建DWD层Doris Sink
     */
    public static DorisSink<String> createDWDDorisSink() {
        return com.lingyun.business.common.sink.DorisBatchSink.createDWDDorisSink(DWD_GOODS_TABLE);
    }

    /**
     * 将ODSGoodsRecord转换为JSON字符串
     */
    public static String odsRecordToJson(ODSGoodsRecord record) {
        Function<ODSGoodsRecord, String> logExtractor = r -> String.format("商品ID=%s, 店铺ID=%s, 站点ID=%s, 标题=%s",
                r.getGoodsId(), r.getMallId(), r.getSiteId(), r.getTitle());
        String json = com.lingyun.business.common.sink.DorisBatchSink.recordToJson(record, ODSGoodsRecord.class, odsCounter,
                logExtractor);
        return json;
    }

    /**
     * 将DWDGoodsRecord转换为JSON字符串
     */
    public static String dwdRecordToJson(DWDGoodsRecord record) {
        Function<DWDGoodsRecord, String> logExtractor = r -> String.format("商品ID=%s, 店铺ID=%s, 站点ID=%s, 标题=%s",
                r.getGoodsId(), r.getMallId(), r.getSiteId(), r.getTitle());
        String json = com.lingyun.business.common.sink.DorisBatchSink.recordToJsonByReflection(record, DWDGoodsRecord.class,
                dwdCounter, logExtractor);
        return json;
    }
}
