package com.lingyun.business.keywordSearch.more.goods.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.lingyun.business.common.model.goods.GoodsSchema;
import com.lingyun.business.common.model.goods.GoodsSchemaConverter;
import com.lingyun.business.common.model.goods.ODSGoodsRecord;
import com.lingyun.business.common.util.JsonUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.Introspector;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.util.Map;

/**
 * ODS层数据处理器 - 关键词搜索查看更多商品数据
 * 使用 RichFlatMapFunction 管理生命周期，避免类加载器泄漏
 */
public class ODSKeyMoreGoodsProcessor extends RichFlatMapFunction<String, ODSGoodsRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ODSKeyMoreGoodsProcessor.class);
    private static final String CONFIG_FILE = "mappingFile/keywordSearch/more/goods/ods_key_more_goods_mapping.json";
    private static final String GOODS_LIST_PREFIX = "data.goods_list[0].";

    /** 预加载的字段映射配置 */
    private transient Map<String, String> fieldMappings;

    /** 预加载的 setter 方法缓存 */
    private transient Map<String, Method> setterCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 预加载字段映射配置
        this.fieldMappings = com.lingyun.business.common.config.FieldMappingLoader.getFieldMappings(CONFIG_FILE);

        // 预加载 setter 方法缓存
        this.setterCache = new java.util.HashMap<>();
        for (Method method : ODSGoodsRecord.class.getMethods()) {
            String name = method.getName();
            if (name.startsWith("set") && name.length() > 3 && method.getParameterCount() == 1) {
                String fieldName = Introspector.decapitalize(name.substring(3));
                setterCache.put(fieldName, method);
                setterCache.put(GoodsSchema.normalizeFieldName(fieldName), method);
                setterCache.put(GoodsSchema.normalizeFieldName(fieldName).toLowerCase(), method);
            }
        }
        LOGGER.info("ODSKeyMoreGoodsProcessor 初始化完成: {} 个字段映射, {} 个setter方法",
                fieldMappings.size(), setterCache.size());
    }

    @Override
    public void close() throws Exception {
        this.fieldMappings = null;
        this.setterCache = null;
        LOGGER.info("ODSKeyMoreGoodsProcessor 资源已释放");
        super.close();
    }

    @Override
    public void flatMap(String jsonStr, Collector<ODSGoodsRecord> out) throws Exception {
        // 空值检查
        if (jsonStr == null || jsonStr.trim().isEmpty()) {
            LOGGER.debug("输入JSON字符串为空");
            return;
        }

        JsonNode rootNode = JsonUtil.parseJson(jsonStr);
        // 空值检查
        if (rootNode == null) {
            LOGGER.warn("JSON解析失败，返回null: {}", jsonStr.length() > 100 ? jsonStr.substring(0, 100) + "..." : jsonStr);
            return;
        }

        JsonNode goodsList = rootNode.path("data").path("goods_list");
        if (goodsList.isArray() && goodsList.size() > 0) {
            for (JsonNode goodsItem : goodsList) {
                out.collect(buildRecord(rootNode, goodsItem));
            }
            return;
        }

        out.collect(buildRecord(rootNode, null));
    }

    private ODSGoodsRecord buildRecord(JsonNode rootNode, JsonNode goodsItem) {
        ODSGoodsRecord record = new ODSGoodsRecord();
        record.setDate(LocalDate.now().toString());

        // 使用预加载的配置设置字段
        if (fieldMappings != null && !fieldMappings.isEmpty()) {
            for (Map.Entry<String, String> entry : fieldMappings.entrySet()) {
                setField(record, entry.getKey(), entry.getValue(), rootNode, goodsItem);
            }
        }

        return record;
    }

    /**
     * 设置字段值
     */
    private void setField(ODSGoodsRecord record, String fieldName, String jsonPath, JsonNode rootNode, JsonNode goodsItem) {
        if (jsonPath == null || jsonPath.isEmpty()) {
            return;
        }

        JsonNode node = getFieldNode(rootNode, goodsItem, jsonPath);
        if (node == null || node.isNull()) {
            return;
        }

        String value = convertToString(node);
        if (value != null && !value.isEmpty()) {
            setFieldValue(record, GoodsSchema.normalizeFieldName(fieldName), value);
        }
    }

    private JsonNode getFieldNode(JsonNode rootNode, JsonNode goodsItem, String jsonPath) {
        if (goodsItem != null && jsonPath.startsWith(GOODS_LIST_PREFIX)) {
            return getNestedNode(goodsItem, jsonPath.substring(GOODS_LIST_PREFIX.length()));
        }
        return getNestedNode(rootNode, jsonPath);
    }

    /**
     * 使用预加载的 setter 方法设置字段值
     */
    private void setFieldValue(Object obj, String fieldName, Object value) {
        String normalizedFieldName = GoodsSchema.normalizeFieldName(fieldName);
        Method setter = setterCache.get(normalizedFieldName);
        if (setter == null) {
            setter = setterCache.get(normalizedFieldName.toLowerCase());
        }
        if (setter != null) {
            try {
                Object converted = GoodsSchemaConverter.convertToParameterType(value, setter.getParameterTypes()[0]);
                if (converted != null) {
                    setter.invoke(obj, converted);
                }
            } catch (Exception e) {
                LOGGER.debug("设置字段 {} 失败: {}", fieldName, e.getMessage());
            }
        }
    }

    /**
     * 获取嵌套节点
     */
    private JsonNode getNestedNode(JsonNode node, String path) {
        if (node == null || path == null || path.isEmpty()) {
            return null;
        }
        JsonNode current = node;
        String[] segments = path.split("\\.");
        for (String segment : segments) {
            if (segment.isEmpty()) {
                return null;
            }
            if (segment.contains("[") && segment.contains("]")) {
                String arrayName = segment.substring(0, segment.indexOf("["));
                String indexStr = segment.substring(segment.indexOf("[") + 1, segment.indexOf("]"));
                try {
                    int index = Integer.parseInt(indexStr);
                    if (current.has(arrayName)) {
                        current = current.get(arrayName);
                        if (current.isArray() && index >= 0 && index < current.size()) {
                            current = current.get(index);
                        } else {
                            return null;
                        }
                    } else {
                        return null;
                    }
                } catch (NumberFormatException e) {
                    return null;
                }
            } else {
                if (current.isArray()) {
                    try {
                        int index = Integer.parseInt(segment);
                        if (index < 0 || index >= current.size()) {
                            return null;
                        }
                        current = current.get(index);
                    } catch (NumberFormatException e) {
                        return null;
                    }
                } else {
                    if (!current.has(segment)) {
                        return null;
                    }
                    current = current.get(segment);
                }
            }
        }
        return current == null || current.isNull() ? null : current;
    }

    /**
     * 将JsonNode转换为String
     */
    private String convertToString(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isObject() || node.isArray()) {
            return node.toString();
        }
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isNumber()) {
            return node.asText();
        }
        if (node.isBoolean()) {
            return String.valueOf(node.asBoolean());
        }
        return node.toString();
    }
}
