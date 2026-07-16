package com.lingyun.business.goodsDetail.mall.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.lingyun.business.common.config.ConfigBasedFieldSetter;
import com.lingyun.business.common.config.FieldMappingLoader;
import com.lingyun.business.common.util.DataCleanUtil;
import com.lingyun.business.common.util.JsonUtil;
import com.lingyun.business.common.model.mall.ODSMallsRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * ODS层数据处理器
 * 使用 RichMapFunction 管理生命周期，避免类加载器泄漏
 */
public class ODSMallsProcessor extends RichMapFunction<String, ODSMallsRecord> {
    private static final long serialVersionUID = 1L;
    private static final String CONFIG_FILE = "mappingFile/goodsDetail/ods_goods_detail_malls_mapping.json";
    private static final Set<String> JSON_STRING_FIELDS = new HashSet<>(Arrays.asList(
            "goodsNumUnit",
            "goodsSalesNumUnit",
            "followerNumUnit",
            "shareInfo",
            "scoreNumInfoList",
            "topShopInfo",
            "optList",
            "mallTags",
            "semiManagedMallTags"
    ));

    private transient Map<String, String> fieldMappings;
    private transient Map<String, Method> setterCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.fieldMappings = FieldMappingLoader.getFieldMappings(CONFIG_FILE);
        this.setterCache = ConfigBasedFieldSetter.initSetterCache(ODSMallsRecord.class);
    }

    @Override
    public void close() throws Exception {
        this.fieldMappings = null;
        this.setterCache = null;
        super.close();
    }

    @Override
    public ODSMallsRecord map(String jsonStr) throws Exception {
        ODSMallsRecord record = new ODSMallsRecord();
        JsonNode rootNode = JsonUtil.parseJson(jsonStr);

        // 商品详情页直接使用根节点，不复用店铺详情页的 _children 提取逻辑
        ConfigBasedFieldSetter<ODSMallsRecord> setter = new ConfigBasedFieldSetter<>(
                rootNode, ODSMallsRecord.class, fieldMappings, setterCache,
                true, null, JSON_STRING_FIELDS);
        setter.setFields(record);
        cleanDorisTypedFields(record);

        return record;
    }

    private void cleanDorisTypedFields(ODSMallsRecord record) {
        record.setGoodsNum(DataCleanUtil.parseSalesNumWithUnitFallback(
                record.getGoodsNum(), record.getGoodsNumUnit()));
        record.setGoodsSalesNum(DataCleanUtil.parseSalesNumWithUnitFallback(
                record.getGoodsSalesNum(), record.getGoodsSalesNumUnit()));
        record.setFollowerNum(DataCleanUtil.parseSalesNumWithUnitFallback(
                record.getFollowerNum(), record.getFollowerNumUnit()));
        record.setReviewNum(DataCleanUtil.parseSalesNum(record.getReviewNum()));
        record.setMallStar(DataCleanUtil.parseDecimal(record.getMallStar()));
        record.setServerTime(DataCleanUtil.parseSalesNum(record.getServerTime()));
    }
}
