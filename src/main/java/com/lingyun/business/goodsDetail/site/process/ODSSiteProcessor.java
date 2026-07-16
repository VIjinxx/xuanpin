package com.lingyun.business.goodsDetail.site.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.lingyun.business.common.config.ConfigBasedFieldSetter;
import com.lingyun.business.common.config.FieldMappingLoader;
import com.lingyun.business.common.util.JsonUtil;
import com.lingyun.business.common.model.site.ODSSiteRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * ODS层数据处理器
 * 使用 RichMapFunction 管理生命周期，避免类加载器泄漏
 */
public class ODSSiteProcessor extends RichMapFunction<String, ODSSiteRecord> {
    private static final long serialVersionUID = 1L;
    private static final String CONFIG_FILE = "mappingFile/goodsDetail/ods_goods_detail_site_mapping.json";

    private transient Map<String, String> fieldMappings;
    private transient Map<String, Method> setterCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.fieldMappings = FieldMappingLoader.getFieldMappings(CONFIG_FILE);
        this.setterCache = ConfigBasedFieldSetter.initSetterCache(ODSSiteRecord.class);
    }

    @Override
    public void close() throws Exception {
        this.fieldMappings = null;
        this.setterCache = null;
        super.close();
    }

    @Override
    public ODSSiteRecord map(String jsonStr) throws Exception {
        ODSSiteRecord record = new ODSSiteRecord();
        JsonNode rootNode = JsonUtil.parseJson(jsonStr);

        ConfigBasedFieldSetter<ODSSiteRecord> setter =
                new ConfigBasedFieldSetter<>(rootNode, ODSSiteRecord.class, fieldMappings, setterCache);
        setter.setFields(record);

        return record;
    }
}

