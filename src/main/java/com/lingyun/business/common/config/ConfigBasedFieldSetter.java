package com.lingyun.business.common.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lingyun.business.common.model.goods.GoodsSchema;
import com.lingyun.business.common.model.goods.GoodsSchemaConverter;
import com.lingyun.business.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.Introspector;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 基于配置的字段设置器
 * 使用实例变量缓存反射方法，避免类加载器泄漏
 * 支持泛型，可处理不同类型的 Record
 */
public class ConfigBasedFieldSetter<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigBasedFieldSetter.class);
    private static final ObjectMapper OBJECT_MAPPER = JsonUtil.getObjectMapper();
    private static final Pattern BRACKET_ARRAY_INDEX_PATTERN = Pattern.compile("\\[(\\d+)]");

    /** 线程安全的日期格式化器 */
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private final Map<String, String> fieldMappings;
    private final JsonNode rootNode;
    /** 实例级别的 setter 方法缓存，随实例生命周期结束而释放 */
    private final Map<String, Method> setterCache;
    private final Class<T> recordClass;
    private final boolean autoTimestamps;
    private final Map<String, String> pathVariables;
    private final Set<String> jsonStringFields;

    public ConfigBasedFieldSetter(JsonNode rootNode, Class<T> recordClass, String configFile) {
        this(rootNode, recordClass, configFile, true);
    }

    public ConfigBasedFieldSetter(JsonNode rootNode, Class<T> recordClass, String configFile, boolean autoTimestamps) {
        this(rootNode, recordClass, configFile, autoTimestamps, Collections.emptyMap());
    }

    public ConfigBasedFieldSetter(JsonNode rootNode, Class<T> recordClass, String configFile,
                                  boolean autoTimestamps, Map<String, String> pathVariables) {
        this(rootNode, recordClass, configFile, autoTimestamps, pathVariables, Collections.emptySet());
    }

    public ConfigBasedFieldSetter(JsonNode rootNode, Class<T> recordClass, String configFile,
                                  boolean autoTimestamps, Map<String, String> pathVariables,
                                  Set<String> jsonStringFields) {
        this(rootNode, recordClass, FieldMappingLoader.getFieldMappings(configFile),
                initSetterCache(recordClass), autoTimestamps, pathVariables, jsonStringFields);
    }

    public ConfigBasedFieldSetter(JsonNode rootNode, Class<T> recordClass,
                                  Map<String, String> fieldMappings, Map<String, Method> setterCache) {
        this(rootNode, recordClass, fieldMappings, setterCache, true);
    }

    public ConfigBasedFieldSetter(JsonNode rootNode, Class<T> recordClass,
                                  Map<String, String> fieldMappings, Map<String, Method> setterCache,
                                  boolean autoTimestamps) {
        this(rootNode, recordClass, fieldMappings, setterCache, autoTimestamps,
                Collections.emptyMap(), Collections.emptySet());
    }

    public ConfigBasedFieldSetter(JsonNode rootNode, Class<T> recordClass,
                                  Map<String, String> fieldMappings, Map<String, Method> setterCache,
                                  boolean autoTimestamps, Map<String, String> pathVariables) {
        this(rootNode, recordClass, fieldMappings, setterCache, autoTimestamps,
                pathVariables, Collections.emptySet());
    }

    public ConfigBasedFieldSetter(JsonNode rootNode, Class<T> recordClass,
                                  Map<String, String> fieldMappings, Map<String, Method> setterCache,
                                  boolean autoTimestamps, Map<String, String> pathVariables,
                                  Set<String> jsonStringFields) {
        this.fieldMappings = fieldMappings == null ? Collections.emptyMap() : fieldMappings;
        this.rootNode = rootNode;
        this.recordClass = recordClass;
        this.autoTimestamps = autoTimestamps;
        this.pathVariables = pathVariables == null ? Collections.emptyMap() : new HashMap<>(pathVariables);
        this.jsonStringFields = normalizeFieldNames(jsonStringFields);
        this.setterCache = setterCache == null ? initSetterCache(recordClass) : setterCache;
    }

    /**
     * 初始化 setter 方法缓存
     */
    public static <T> Map<String, Method> initSetterCache(Class<T> recordClass) {
        Map<String, Method> cache = new HashMap<>();
        for (Method method : recordClass.getMethods()) {
            String name = method.getName();
            if (name.startsWith("set") && name.length() > 3 && method.getParameterCount() == 1) {
                // setFieldName -> fieldName
                String fieldName = Introspector.decapitalize(name.substring(3));
                cache.put(fieldName, method);
                cache.put(GoodsSchema.normalizeFieldName(fieldName), method);
                cache.put(GoodsSchema.normalizeFieldName(fieldName).toLowerCase(), method);
            }
        }
        return cache;
    }

    /**
     * 根据配置设置所有字段
     */
    public void setFields(T record) throws Exception {
        if (fieldMappings != null && !fieldMappings.isEmpty()) {
            for (Map.Entry<String, String> entry : fieldMappings.entrySet()) {
                setField(record, GoodsSchema.normalizeFieldName(entry.getKey()),
                        resolvePathVariables(entry.getValue()));
            }
        }
        // 使用线程安全的 DateTimeFormatter
        if (autoTimestamps) {
            setFieldValue(record, "lastUpdateTime", LocalDateTime.now().format(DATE_TIME_FORMATTER));
            setFieldValue(record, "date", LocalDateTime.now().format(DATE_FORMATTER));
        }
    }

    private String resolvePathVariables(String jsonPath) {
        if (jsonPath == null || jsonPath.isEmpty() || pathVariables.isEmpty()) {
            return jsonPath;
        }

        String resolvedPath = jsonPath;
        for (Map.Entry<String, String> entry : pathVariables.entrySet()) {
            resolvedPath = resolvedPath.replace("[" + entry.getKey() + "]", "[" + entry.getValue() + "]");
            resolvedPath = resolvedPath.replace("${" + entry.getKey() + "}", entry.getValue());
            resolvedPath = resolvedPath.replace("{" + entry.getKey() + "}", entry.getValue());
        }
        return resolvedPath;
    }

    private Set<String> normalizeFieldNames(Set<String> fieldNames) {
        if (fieldNames == null || fieldNames.isEmpty()) {
            return Collections.emptySet();
        }

        Set<String> normalized = new HashSet<>();
        for (String fieldName : fieldNames) {
            if (fieldName == null || fieldName.isEmpty()) {
                continue;
            }
            String normalizedFieldName = GoodsSchema.normalizeFieldName(fieldName);
            normalized.add(normalizedFieldName);
            normalized.add(normalizedFieldName.toLowerCase());
        }
        return normalized;
    }

    private boolean isJsonStringField(String fieldName) {
        if (jsonStringFields.isEmpty()) {
            return false;
        }
        String normalizedFieldName = GoodsSchema.normalizeFieldName(fieldName);
        return jsonStringFields.contains(normalizedFieldName)
                || jsonStringFields.contains(normalizedFieldName.toLowerCase());
    }

    /**
     * 设置字段
     */
    private void setField(T record, String fieldName, String jsonPath) throws Exception {
        if (jsonPath == null || jsonPath.isEmpty()) {
            return;
        }

        Method setter = findSetter(fieldName);
        if (setter == null) {
            LOGGER.debug("未找到字段 {} 的 setter 方法", fieldName);
            return;
        }

        Class<?> parameterType = setter.getParameterTypes()[0];
        boolean jsonField = JsonNode.class.equals(parameterType);
        boolean jsonStringField = String.class.equals(parameterType) && isJsonStringField(fieldName);
        JsonNode node = getNested(rootNode, jsonPath, jsonField || jsonStringField);
        if (node == null || node.isNull()) {
            return;
        }

        Object value = (jsonField || jsonStringField) ? normalizeFlattenedNode(node, jsonPath) : node;
        if (value != null) {
            setFieldValue(record, fieldName, value, setter);
        }
    }

    /**
     * 将JsonNode转换为String
     */
    private String convertToString(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }

        // 如果是对象或数组，转换为JSON字符串
        if (node.isObject() || node.isArray()) {
            return node.toString();
        }

        // 其他类型（文本、数字、布尔值）转换为文本
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isNumber()) {
            return node.asText();
        }
        if (node.isBoolean()) {
            return String.valueOf(node.asBoolean());
        }

        // 默认转换为字符串
        return node.toString();
    }

    /**
     * 使用实例缓存的 setter 方法设置字段值
     */
    private void setFieldValue(Object obj, String fieldName, Object value) throws Exception {
        Method setter = findSetter(fieldName);
        if (setter != null) {
            setFieldValue(obj, fieldName, value, setter);
        } else {
            LOGGER.debug("未找到字段 {} 的 setter 方法", fieldName);
        }
    }

    private Method findSetter(String fieldName) {
        String normalizedFieldName = GoodsSchema.normalizeFieldName(fieldName);
        Method setter = setterCache.get(normalizedFieldName);
        if (setter == null) {
            setter = setterCache.get(normalizedFieldName.toLowerCase());
        }
        return setter;
    }

    private void setFieldValue(Object obj, String fieldName, Object value, Method setter) throws Exception {
        Object converted = GoodsSchemaConverter.convertToParameterType(value, setter.getParameterTypes()[0]);
        if (converted != null) {
            setter.invoke(obj, converted);
        }
    }

    // ========== 辅助方法 ==========

    private JsonNode getNested(JsonNode node, String path, boolean allowFlattenedRebuild) {
        if (node == null || path == null || path.isEmpty()) {
            return null;
        }

        JsonNode directNode = getDirectField(node, path);
        if (directNode != null) {
            if (allowFlattenedRebuild) {
                JsonNode rebuilt = rebuildFromFlattenedFields(node, path, false);
                if (rebuilt != null) {
                    return rebuilt;
                }
            }
            return directNode;
        }

        JsonNode current = node;
        String dottedPath = toDottedArrayPath(path);
        String[] segments = dottedPath.split("\\.");
        for (String segment : segments) {
            if (segment.isEmpty()) {
                return null;
            }

            JsonNode fullFlatNode = getDirectField(current, dottedPath);
            if (fullFlatNode != null) {
                return fullFlatNode;
            }

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
                    return allowFlattenedRebuild ? rebuildFromFlattenedFields(node, path, false) : null;
                }
                current = current.get(segment);
            }
        }
        return current == null || current.isNull() ? null : current;
    }

    private JsonNode getDirectField(JsonNode node, String path) {
        if (node == null || !node.isObject() || path == null || path.isEmpty()) {
            return null;
        }
        if (node.has(path)) {
            return node.get(path);
        }
        String dottedPath = toDottedArrayPath(path);
        if (!dottedPath.equals(path) && node.has(dottedPath)) {
            return node.get(dottedPath);
        }
        return null;
    }

    private JsonNode normalizeFlattenedNode(JsonNode node, String path) {
        if (node == null || !node.isObject()) {
            return node;
        }

        JsonNode rebuilt = rebuildFromFlattenedFields(node, path, true);
        if (rebuilt != null) {
            return rebuilt;
        }

        return hasOnlyArrayIndexKeys(node) ? compactNumericObjects(node, true) : node;
    }

    private JsonNode rebuildFromFlattenedFields(JsonNode source, String path, boolean allowRelativeKeys) {
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
            String relativePath = flattenedRelativePath(entry.getKey(), prefix, allowRelativeKeys);
            if (relativePath == null || relativePath.isEmpty()) {
                continue;
            }
            putFlattenedValue(rebuilt, relativePath, entry.getValue());
            found = true;
        }

        return found ? compactNumericObjects(rebuilt, true) : null;
    }

    private String flattenedRelativePath(String key, String prefix, boolean allowRelativeKeys) {
        String dottedKey = toDottedArrayPath(key);
        if (dottedKey.startsWith(prefix)) {
            return dottedKey.substring(prefix.length());
        }
        if (allowRelativeKeys && startsWithArrayIndex(dottedKey)) {
            return dottedKey;
        }
        return null;
    }

    private boolean startsWithArrayIndex(String path) {
        int dotIndex = path.indexOf('.');
        String firstSegment = dotIndex >= 0 ? path.substring(0, dotIndex) : path;
        return parseArrayIndex(firstSegment) != null;
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

    private boolean isNonNegativeInteger(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private Integer parseArrayIndex(String value) {
        if (!isNonNegativeInteger(value)) {
            return null;
        }
        try {
            long index = Long.parseLong(value);
            if (index > Integer.MAX_VALUE) {
                return null;
            }
            return (int) index;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private boolean hasOnlyArrayIndexKeys(JsonNode node) {
        if (node == null || !node.isObject()) {
            return false;
        }
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        boolean hasField = false;
        TreeMap<Integer, JsonNode> numericChildren = new TreeMap<>();
        while (fields.hasNext()) {
            hasField = true;
            Map.Entry<String, JsonNode> entry = fields.next();
            Integer index = parseArrayIndex(entry.getKey());
            if (index == null) {
                return false;
            }
            numericChildren.put(index, entry.getValue());
        }
        return hasField && isZeroBasedContiguous(numericChildren);
    }

    private String toDottedArrayPath(String path) {
        Matcher matcher = BRACKET_ARRAY_INDEX_PATTERN.matcher(path);
        return matcher.replaceAll(".$1");
    }
}

