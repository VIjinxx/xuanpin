package com.lingyun.business.goodsDetail.site.sink;

import com.lingyun.business.common.model.site.DWDSiteRecord;
import com.lingyun.business.common.model.site.ODSSiteRecord;
import org.apache.doris.flink.sink.DorisSink;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Doris批量写入Sink - Site适配器
 * 提供site特定的方法，内部使用common包中的通用实现
 */
public class DorisBatchSink {

    /** 采样计数器，用于采样日志 */
    private static final AtomicLong odsCounter = new AtomicLong(0);
    private static final AtomicLong dwdCounter = new AtomicLong(0);

    // 表名常量
    private static final String ODS_SITE_TABLE = "ods_site_raw";
    private static final String DWD_SITE_TABLE = "dwd_site_base";

    /**
     * 创建ODS层Doris Sink
     */
    public static DorisSink<String> createODSDorisSink() {
        return com.lingyun.business.common.sink.DorisBatchSink.createODSDorisSink(ODS_SITE_TABLE);
    }

    /**
     * 创建DWD层Doris Sink
     */
    public static DorisSink<String> createDWDDorisSink() {
        return com.lingyun.business.common.sink.DorisBatchSink.createDWDDorisSink(DWD_SITE_TABLE);
    }

    /**
     * 将ODSSiteRecord转换为JSON字符串
     */
    public static String odsSiteRecordToJson(ODSSiteRecord record) {
        Function<ODSSiteRecord, String> logExtractor = r -> 
            String.format("站点ID=%s, 区域ID=%s, 区域名称=%s", 
                r.getSiteId(), r.getRegionId(), r.getRegionName());
        String json = com.lingyun.business.common.sink.DorisBatchSink.recordToJson(record, ODSSiteRecord.class, odsCounter, logExtractor);
        return json;
    }

    /**
     * 将DWDSiteRecord转换为JSON字符串
     */
    public static String dwdSiteRecordToJson(DWDSiteRecord record) {
        Function<DWDSiteRecord, String> logExtractor = r -> 
            String.format("站点ID=%s, 区域ID=%s, 区域名称=%s", 
                r.getSiteId(), r.getRegionId(), r.getRegionName());
        String json = com.lingyun.business.common.sink.DorisBatchSink.recordToJsonByReflection(record, DWDSiteRecord.class, dwdCounter, logExtractor);
        return json;
    }
}

