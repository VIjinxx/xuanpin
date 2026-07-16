package com.lingyun.business.keywordSearch.html.category.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lingyun.business.common.config.ConfigBasedFieldSetter;
import com.lingyun.business.common.config.FieldMappingLoader;
import com.lingyun.business.common.util.JsonUtil;
import com.lingyun.business.common.model.category.ODSCategoryRecord;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * ODS层数据处理器 - 关键词搜索页类目数据
 * 使用 RichFlatMapFunction 管理生命周期，避免类加载器泄漏
 */
public class ODSKeyCategoryProcessor extends RichFlatMapFunction<String, ODSCategoryRecord> {
    private static final long serialVersionUID = 1L;
    private static final String CONFIG_FILE = "mappingFile/keywordSearch/html/category/ods_category_mapping.json";

    private transient ObjectMapper objectMapper;
    private transient Map<String, String> fieldMappings;
    private transient Map<String, Method> setterCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.objectMapper = new ObjectMapper();
        this.fieldMappings = FieldMappingLoader.getFieldMappings(CONFIG_FILE);
        this.setterCache = ConfigBasedFieldSetter.initSetterCache(ODSCategoryRecord.class);
    }

    @Override
    public void close() throws Exception {
        this.objectMapper = null;
        this.fieldMappings = null;
        this.setterCache = null;
        super.close();
    }

    @Override
    public void flatMap(String jsonStr, Collector<ODSCategoryRecord> out) throws Exception {
        if (jsonStr == null || jsonStr.trim().isEmpty()) {
            return;
        }

        JsonNode rootNode = JsonUtil.parseJson(jsonStr);
        if (rootNode == null) {
            return;
        }

        JsonNode goodsList = rootNode.path("goodsList");
        if (!goodsList.isArray() || goodsList.isEmpty()) {
            out.collect(buildRecord(rootNode));
            return;
        }

        for (JsonNode goodsItem : goodsList) {
            ObjectNode mergedNode = objectMapper.createObjectNode();
            rootNode.fields().forEachRemaining(entry -> {
                if (!"goodsList".equals(entry.getKey())) {
                    mergedNode.set(entry.getKey(), entry.getValue());
                }
            });
            mergedNode.putArray("goodsList").add(goodsItem);

            out.collect(buildRecord(mergedNode));
        }
    }

    private ODSCategoryRecord buildRecord(JsonNode rootNode) throws Exception {
        ODSCategoryRecord record = new ODSCategoryRecord();
        ConfigBasedFieldSetter<ODSCategoryRecord> setter =
                new ConfigBasedFieldSetter<>(rootNode, ODSCategoryRecord.class, fieldMappings, setterCache);
        setter.setFields(record);
        return record;
    }
}
