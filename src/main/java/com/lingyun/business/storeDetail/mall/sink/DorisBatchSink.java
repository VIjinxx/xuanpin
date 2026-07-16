package com.lingyun.business.storeDetail.mall.sink;
import com.lingyun.business.common.model.mall.DWDMallsRecord;
import com.lingyun.business.common.model.mall.ODSMallsRecord;
import com.lingyun.business.common.util.DataCleanUtil;
import org.apache.doris.flink.sink.DorisSink;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Doris批量写入Sink - Mall适配器
 * 提供mall特定的方法，内部使用common包中的通用实现
 */
public class DorisBatchSink {
    
    /** 采样计数器，用于采样日志 */
    private static final AtomicLong odsCounter = new AtomicLong(0);
    private static final AtomicLong dwdCounter = new AtomicLong(0);

    // 表名常量
    private static final String ODS_MALLS_TABLE = "ods_malls_raw";
    private static final String DWD_MALLS_TABLE = "dwd_malls_base";
    private static final Set<String> ODS_EXCLUDED_FIELDS =
            Collections.singleton("lastUpdateTime");
    private static final Set<String> ODS_INTEGER_FIELDS = new HashSet<>();

    static {
        Collections.addAll(ODS_INTEGER_FIELDS,
                "siteId",
                "appCode",
                "goodsNum",
                "goodsSalesNum",
                "followerNum",
                "reviewNum",
                "marketPriceType",
                "serverTime");
    }

    /**
     * 创建ODS层Doris Sink
     */
    public static DorisSink<String> createODSDorisSink() {
        return com.lingyun.business.common.sink.DorisBatchSink.createODSDorisSink(ODS_MALLS_TABLE);
    }

    /**
     * 创建DWD层Doris Sink
     */
    public static DorisSink<String> createDWDDorisSink() {
        return com.lingyun.business.common.sink.DorisBatchSink.createDWDDorisSink(DWD_MALLS_TABLE);
    }

    /**
     * 将ODSMallsRecord转换为JSON字符串
     */
    public static String odsRecordToJson(ODSMallsRecord record) {
        Function<ODSMallsRecord, String> logExtractor = r -> 
            String.format("店铺ID=%s, 站点ID=%s, 店铺名称=%s", 
                r.getMallId(), r.getSiteId(), r.getMallName());
        String json = com.lingyun.business.common.sink.DorisBatchSink.recordToJsonByReflection(
                record,
                ODSMallsRecord.class,
                odsCounter,
                logExtractor,
                ODS_EXCLUDED_FIELDS,
                DorisBatchSink::convertOdsValue);
        return json;
    }

    /**
     * 将DWDMallsRecord转换为JSON字符串
     */
    public static String dwdRecordToJson(DWDMallsRecord record) {
        Function<DWDMallsRecord, String> logExtractor = r -> 
            String.format("店铺ID=%s, 站点ID=%s, 店铺名称=%s", 
                r.getMallId(), r.getSiteId(), r.getMallName());
        String json = com.lingyun.business.common.sink.DorisBatchSink.recordToJsonByReflection(record, DWDMallsRecord.class, dwdCounter, logExtractor);
        return json;
    }

    private static Object convertOdsValue(String fieldName, Object value) {
        if (value == null) {
            return null;
        }
        if (ODS_INTEGER_FIELDS.contains(fieldName)) {
            return toLong(DataCleanUtil.parseSalesNum(String.valueOf(value)));
        }
        if ("mallStar".equals(fieldName)) {
            String decimal = DataCleanUtil.parseDecimal(String.valueOf(value));
            return decimal == null ? null : new BigDecimal(decimal);
        }
        if ("mallReviewLabelShow".equals(fieldName) || "isExpire".equals(fieldName)) {
            return toBoolean(String.valueOf(value));
        }
        return value;
    }

    private static Long toLong(String value) {
        return value == null ? null : Long.valueOf(value);
    }

    private static Boolean toBoolean(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim().toLowerCase();
        if ("true".equals(normalized) || "1".equals(normalized) || "yes".equals(normalized)) {
            return Boolean.TRUE;
        }
        if ("false".equals(normalized) || "0".equals(normalized) || "no".equals(normalized)) {
            return Boolean.FALSE;
        }
        return null;
    }
}
