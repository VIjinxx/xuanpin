package com.lingyun.business.goodsDetail.goods.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lingyun.business.common.config.ConfigBasedFieldSetter;
import com.lingyun.business.common.config.FieldMappingLoader;
import com.lingyun.business.common.model.goods.GoodsSchema;
import com.lingyun.business.common.model.goods.GoodsSchemaConverter;
import com.lingyun.business.common.model.goods.ODSGoodsRecord;
import com.lingyun.business.common.util.JsonUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ODS层商品数据处理器
 * 使用 RichFlatMapFunction 管理生命周期,每条商品详情页数据生成一条商品记录。
 * 每个字段按映射来源顺序取第一个非空值。
 */
public class ODSGoodsProcessor extends RichFlatMapFunction<String, ODSGoodsRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ODSGoodsProcessor.class);
    private static final ObjectMapper OBJECT_MAPPER = JsonUtil.getObjectMapper();
    private static final Pattern BRACKET_ARRAY_INDEX_PATTERN = Pattern.compile("\\[([0-9]+|i)]");

    private static final String CONFIG_FILE = "mappingFile/goodsDetail/ods_goods_detail_goods_mapping.json";
    private static final int JSON_PREVIEW_LENGTH = 200;

    private transient Map<String, List<String>> fieldMappings;
    private transient Map<String, Method> setterCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ensureMappingsLoaded();
        this.setterCache = ConfigBasedFieldSetter.initSetterCache(ODSGoodsRecord.class);
        LOGGER.info("ODSGoodsProcessor 初始化完成,映射配置: {}", CONFIG_FILE);
    }

    @Override
    public void close() throws Exception {
        this.fieldMappings = null;
        this.setterCache = null;
        LOGGER.info("ODSGoodsProcessor 资源已释放");
        super.close();
    }

    @Override
    public void flatMap(String jsonStr, Collector<ODSGoodsRecord> out) throws Exception {
        ODSGoodsRecord record = process(jsonStr);
        if (record != null) {
            out.collect(record);
        }
    }

    public ODSGoodsRecord map(String jsonStr) throws Exception {
        return process(jsonStr);
    }

    private ODSGoodsRecord process(String jsonStr) throws Exception {
        if (jsonStr == null || jsonStr.trim().isEmpty()) {
            return null;
        }

        ensureMappingsLoaded();
        JsonNode rootNode = JsonUtil.parseJson(jsonStr);
        if (rootNode == null) {
            LOGGER.warn("解析JSON失败,跳过商品处理: inputLength={}, preview={}",
                    jsonStr.length(), previewJson(jsonStr));
            return null;
        }

        ODSGoodsRecord record = buildRecord(rootNode);
        if (record.getGoodsId() == null) {
            LOGGER.warn("ODS商品解析结果缺少 goodsId,跳过记录: inputLength={}, preview={}",
                    jsonStr.length(), previewJson(jsonStr));
            return null;
        }

        LOGGER.info("ODS商品解析完成: goodsId={}, mappedFields={}",
                record.getGoodsId(), fieldMappings.size());
        return record;
    }

    private void ensureMappingsLoaded() {
        if (fieldMappings == null) {
            fieldMappings = FieldMappingLoader.getFieldMappingSources(CONFIG_FILE);
        }
    }

    private ODSGoodsRecord buildRecord(JsonNode rootNode) {
        ODSGoodsRecord record = new ODSGoodsRecord();
        record.setDate(LocalDate.now().toString());

        for (Map.Entry<String, List<String>> entry : fieldMappings.entrySet()) {
            String fieldName = GoodsSchema.normalizeFieldName(entry.getKey());
            if ("date".equals(fieldName)) {
                continue;
            }

            JsonNode value = resolveFirstNonEmptySource(rootNode, fieldName, entry.getValue());
            if (!isNonEmpty(value)) {
                continue;
            }
            setField(record, fieldName, value);
        }

        return record;
    }

    private JsonNode resolveFirstNonEmptySource(
            JsonNode rootNode,
            String fieldName,
            List<String> sources) {
        if (sources == null || sources.isEmpty()) {
            return null;
        }

        for (String source : sources) {
            String sourcePath = cleanSourcePath(source);
            if (sourcePath.isEmpty()) {
                continue;
            }

            JsonNode value = normalizeMappedValue(
                    fieldName,
                    resolvePath(rootNode, sourcePath, true));
            if (!isEligibleCandidate(fieldName, value)) {
                value = normalizeMappedValue(
                        fieldName,
                        resolveFirstPathMatch(rootNode, splitPath(sourcePath), 0));
            }
            if (isEligibleCandidate(fieldName, value)) {
                return value;
            }
        }
        return null;
    }

    private JsonNode normalizeMappedValue(String fieldName, JsonNode value) {
        if ("salesTipText".equals(fieldName) && value != null && value.isTextual()) {
            ArrayNode normalizedValue = OBJECT_MAPPER.createArrayNode();
            normalizedValue.add(value.asText());
            return normalizedValue;
        }
        return value;
    }

    private boolean isEligibleCandidate(String fieldName, JsonNode value) {
        if (!isNonEmpty(value)) {
            return false;
        }
        if ("title".equals(fieldName)
                && value.isTextual()
                && "Temu".equals(value.asText().trim())) {
            return false;
        }

        GoodsSchema.FieldDef field = GoodsSchema.odsField(fieldName);
        if (field == null) {
            return true;
        }

        Object converted = GoodsSchemaConverter.convertJsonNode(value, field.getType());
        if (converted == null) {
            return false;
        }
        return !(converted instanceof String)
                || !((String) converted).trim().isEmpty();
    }

    private String cleanSourcePath(String sourcePath) {
        return sourcePath == null ? "" : sourcePath.trim();
    }

    private JsonNode resolveFirstPathMatch(JsonNode current, String[] segments, int index) {
        if (current == null || current.isNull() || current.isMissingNode()) {
            return null;
        }
        if (index >= segments.length) {
            return isNonEmpty(current) ? current : null;
        }

        String segment = segments[index];
        if (segment.isEmpty()) {
            return null;
        }

        String remainingPath = joinSegments(segments, index, segments.length);
        JsonNode directNode = getDirectField(current, remainingPath);
        if (isNonEmpty(directNode)) {
            return directNode;
        }

        if ("i".equals(segment)) {
            return resolveFirstFromChildren(current, segments, index + 1);
        }

        if (current.isArray()) {
            Integer arrayIndex = parseArrayIndex(segment);
            if (arrayIndex != null) {
                if (arrayIndex < 0 || arrayIndex >= current.size()) {
                    return null;
                }
                return resolveFirstPathMatch(current.get(arrayIndex), segments, index + 1);
            }
            return resolveFirstFromChildren(current, segments, index);
        }

        if (!current.isObject()) {
            return null;
        }

        if ("value".equals(segment) && !current.has(segment)) {
            Iterator<Map.Entry<String, JsonNode>> fields = current.fields();
            while (fields.hasNext()) {
                JsonNode value = resolveFirstPathMatch(fields.next().getValue(), segments, index + 1);
                if (isNonEmpty(value)) {
                    return value;
                }
            }
            return null;
        }

        if (!current.has(segment)) {
            return null;
        }
        return resolveFirstPathMatch(current.get(segment), segments, index + 1);
    }

    private JsonNode resolveFirstFromChildren(JsonNode current, String[] segments, int nextIndex) {
        if (current.isArray()) {
            for (JsonNode item : current) {
                JsonNode value = resolveFirstPathMatch(item, segments, nextIndex);
                if (isNonEmpty(value)) {
                    return value;
                }
            }
            return null;
        }

        if (current.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = current.fields();
            while (fields.hasNext()) {
                JsonNode value = resolveFirstPathMatch(fields.next().getValue(), segments, nextIndex);
                if (isNonEmpty(value)) {
                    return value;
                }
            }
        }
        return null;
    }

    private boolean isNonEmpty(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return false;
        }
        if (node.isTextual()) {
            return !node.asText().trim().isEmpty();
        }
        if (node.isArray() || node.isObject()) {
            return node.size() > 0;
        }
        return true;
    }

    private void setField(ODSGoodsRecord record, String fieldName, JsonNode value) {
        Method setter = findSetter(fieldName);
        if (setter == null) {
            LOGGER.debug("未找到字段 {} 的 setter 方法", fieldName);
            return;
        }

        try {
            Object converted = GoodsSchemaConverter.convertToParameterType(value, setter.getParameterTypes()[0]);
            if (converted != null) {
                setter.invoke(record, converted);
            }
        } catch (Exception e) {
            LOGGER.debug("商品字段 {} 设置失败: {}", fieldName, e.getMessage());
        }
    }

    private Method findSetter(String fieldName) {
        if (setterCache == null) {
            setterCache = ConfigBasedFieldSetter.initSetterCache(ODSGoodsRecord.class);
        }
        String normalizedFieldName = GoodsSchema.normalizeFieldName(fieldName);
        Method setter = setterCache.get(normalizedFieldName);
        if (setter == null) {
            setter = setterCache.get(normalizedFieldName.toLowerCase());
        }
        return setter;
    }

    private String[] splitPath(String path) {
        String dottedPath = toDottedArrayPath(path);
        return dottedPath == null || dottedPath.isEmpty() ? new String[0] : dottedPath.split("\\.");
    }

    private String joinSegments(String[] segments, int startInclusive, int endExclusive) {
        StringBuilder builder = new StringBuilder();
        for (int i = startInclusive; i < endExclusive; i++) {
            if (segments[i].isEmpty()) {
                continue;
            }
            if (builder.length() > 0) {
                builder.append('.');
            }
            builder.append(segments[i]);
        }
        return builder.toString();
    }

    private JsonNode resolvePath(JsonNode node, String path, boolean allowFlattenedRebuild) {
        if (node == null || path == null || path.isEmpty()) {
            return null;
        }

        JsonNode directNode = getDirectField(node, path);
        if (directNode != null) {
            if (allowFlattenedRebuild) {
                JsonNode rebuilt = rebuildFromFlattenedFields(node, path);
                return rebuilt == null ? directNode : rebuilt;
            }
            return directNode;
        }

        String dottedPath = toDottedArrayPath(path);
        JsonNode current = node;
        String[] segments = dottedPath.split("\\.");
        for (String segment : segments) {
            if (segment.isEmpty() || current == null || current.isNull() || "i".equals(segment)) {
                return null;
            }

            JsonNode fullFlatNode = getDirectField(current, dottedPath);
            if (fullFlatNode != null) {
                return fullFlatNode;
            }

            if (current.isArray()) {
                Integer index = parseArrayIndex(segment);
                if (index == null || index < 0 || index >= current.size()) {
                    return allowFlattenedRebuild ? rebuildFromFlattenedFields(node, path) : null;
                }
                current = current.get(index);
            } else {
                if (current.isObject() && "value".equals(segment) && !current.has(segment)) {
                    current = collectObjectValues(current);
                } else if (!current.has(segment)) {
                    return allowFlattenedRebuild ? rebuildFromFlattenedFields(node, path) : null;
                } else {
                    current = current.get(segment);
                }
            }
        }
        return current == null || current.isNull() ? null : current;
    }

    private ArrayNode collectObjectValues(JsonNode objectNode) {
        ArrayNode values = OBJECT_MAPPER.createArrayNode();
        Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
        while (fields.hasNext()) {
            values.add(fields.next().getValue());
        }
        return values;
    }

    private JsonNode getDirectField(JsonNode node, String path) {
        if (node == null || !node.isObject() || path == null || path.isEmpty()) {
            return null;
        }
        if (node.has(path)) {
            return node.get(path);
        }
        String dottedPath = toDottedArrayPath(path);
        return node.has(dottedPath) ? node.get(dottedPath) : null;
    }

    private JsonNode rebuildFromFlattenedFields(JsonNode source, String path) {
        if (source == null || !source.isObject() || path == null || path.isEmpty()) {
            return null;
        }

        String dottedPath = toDottedArrayPath(path);
        String prefix = dottedPath + ".";
        ObjectNode rebuilt = OBJECT_MAPPER.createObjectNode();
        boolean found = false;

        Iterator<Map.Entry<String, JsonNode>> fields = source.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String dottedKey = toDottedArrayPath(entry.getKey());
            if (!dottedKey.startsWith(prefix)) {
                continue;
            }
            String relativePath = dottedKey.substring(prefix.length());
            if (relativePath.isEmpty()) {
                continue;
            }
            putFlattenedValue(rebuilt, relativePath, entry.getValue());
            found = true;
        }

        return found ? compactNumericObjects(rebuilt, true) : null;
    }

    private void putFlattenedValue(ObjectNode root, String relativePath, JsonNode value) {
        String[] segments = relativePath.split("\\.");
        ObjectNode current = root;
        for (int i = 0; i < segments.length; i++) {
            String segment = segments[i];
            if (segment.isEmpty()) {
                return;
            }
            if (i == segments.length - 1) {
                current.set(segment, value.deepCopy());
                return;
            }

            JsonNode next = current.get(segment);
            if (!(next instanceof ObjectNode)) {
                ObjectNode child = OBJECT_MAPPER.createObjectNode();
                current.set(segment, child);
                current = child;
            } else {
                current = (ObjectNode) next;
            }
        }
    }

    private JsonNode compactNumericObjects(JsonNode node, boolean topLevel) {
        if (node == null || node.isNull()) {
            return node;
        }
        if (node.isArray()) {
            ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
            for (JsonNode item : node) {
                arrayNode.add(compactNumericObjects(item, false));
            }
            return arrayNode;
        }
        if (!node.isObject()) {
            return node.deepCopy();
        }

        ObjectNode objectNode = (ObjectNode) node;
        TreeMap<Integer, JsonNode> numericChildren = new TreeMap<>();
        boolean hasField = false;
        boolean allNumeric = true;
        Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
        while (fields.hasNext()) {
            hasField = true;
            Map.Entry<String, JsonNode> entry = fields.next();
            Integer index = parseArrayIndex(entry.getKey());
            if (index == null) {
                allNumeric = false;
                break;
            }
            numericChildren.put(index, entry.getValue());
        }

        if (hasField && allNumeric && isZeroBasedContiguous(numericChildren)) {
            if (topLevel && numericChildren.size() == 1) {
                return compactNumericObjects(numericChildren.firstEntry().getValue(), false);
            }
            ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
            for (JsonNode child : numericChildren.values()) {
                arrayNode.add(compactNumericObjects(child, false));
            }
            return arrayNode;
        }

        ObjectNode compacted = OBJECT_MAPPER.createObjectNode();
        fields = objectNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            compacted.set(entry.getKey(), compactNumericObjects(entry.getValue(), false));
        }
        return compacted;
    }

    private boolean isZeroBasedContiguous(TreeMap<Integer, JsonNode> numericChildren) {
        int expected = 0;
        for (Integer index : numericChildren.keySet()) {
            if (index != expected) {
                return false;
            }
            expected++;
        }
        return !numericChildren.isEmpty();
    }

    private Integer parseArrayIndex(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return null;
            }
        }
        try {
            long index = Long.parseLong(value);
            return index > Integer.MAX_VALUE ? null : (int) index;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String toDottedArrayPath(String path) {
        if (path == null) {
            return null;
        }
        Matcher matcher = BRACKET_ARRAY_INDEX_PATTERN.matcher(path);
        return matcher.replaceAll(".$1");
    }

    private String previewJson(String jsonStr) {
        String preview = jsonStr.replaceAll("\\s+", " ").trim();
        if (preview.length() <= JSON_PREVIEW_LENGTH) {
            return preview;
        }
        return preview.substring(0, JSON_PREVIEW_LENGTH) + "...";
    }

}
