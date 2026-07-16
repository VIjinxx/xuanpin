package com.lingyun.business.common.util;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * JSON 工具类
 * 提供JSON解析、节点遍历等通用方法
 *
 * @author wxx
 */
public class JsonUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonUtil.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectMapper RELAXED_OBJECT_MAPPER = JsonMapper.builder()
            .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
            .enable(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES)
            .enable(JsonReadFeature.ALLOW_TRAILING_COMMA)
            .enable(JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER)
            .build();
    private static final int JSON_PREVIEW_LENGTH = 300;

    private JsonUtil() {
        // 工具类禁止实例化
    }

    /**
     * 获取共享的ObjectMapper实例
     * 注意：此实例是线程安全的，但不要修改其配置
     *
     * @return ObjectMapper实例
     */
    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }

    /**
     * 解析 JSON 字符串为 JsonNode
     *
     * @param jsonStr JSON字符串
     * @return JsonNode对象，解析失败返回null
     */
    public static JsonNode parseJson(String jsonStr) {
        if (jsonStr == null || jsonStr.trim().isEmpty()) {
            return null;
        }
        String trimmedJson = jsonStr.trim();
        try {
            return OBJECT_MAPPER.readTree(trimmedJson);
        } catch (Exception strictException) {
            String relaxedJson = normalizePythonLiteralTokens(trimmedJson);
            try {
                JsonNode node = RELAXED_OBJECT_MAPPER.readTree(relaxedJson);
                LOGGER.warn("JSON严格解析失败,已使用兼容模式解析成功: strictError={}, length={}, preview={}",
                        summarizeException(strictException), trimmedJson.length(), previewJson(trimmedJson));
                return node;
            } catch (Exception relaxedException) {
                LOGGER.warn("JSON解析失败: strictError={}, relaxedError={}, length={}, preview={}",
                        summarizeException(strictException), summarizeException(relaxedException),
                        trimmedJson.length(), previewJson(trimmedJson));
                return null;
            }
        }
    }

    private static String normalizePythonLiteralTokens(String input) {
        StringBuilder result = new StringBuilder(input.length());
        boolean inSingleQuotedString = false;
        boolean inDoubleQuotedString = false;
        boolean escaped = false;

        for (int i = 0; i < input.length(); ) {
            char current = input.charAt(i);

            if (inSingleQuotedString) {
                result.append(current);
                if (escaped) {
                    escaped = false;
                } else if (current == '\\') {
                    escaped = true;
                } else if (current == '\'') {
                    inSingleQuotedString = false;
                }
                i++;
                continue;
            }

            if (inDoubleQuotedString) {
                result.append(current);
                if (escaped) {
                    escaped = false;
                } else if (current == '\\') {
                    escaped = true;
                } else if (current == '"') {
                    inDoubleQuotedString = false;
                }
                i++;
                continue;
            }

            if (current == '\'') {
                inSingleQuotedString = true;
                result.append(current);
                i++;
                continue;
            }
            if (current == '"') {
                inDoubleQuotedString = true;
                result.append(current);
                i++;
                continue;
            }

            if (matchesToken(input, i, "None")) {
                result.append("null");
                i += 4;
                continue;
            }
            if (matchesToken(input, i, "True")) {
                result.append("true");
                i += 4;
                continue;
            }
            if (matchesToken(input, i, "False")) {
                result.append("false");
                i += 5;
                continue;
            }

            result.append(current);
            i++;
        }

        return result.toString();
    }

    private static boolean matchesToken(String input, int index, String token) {
        if (!input.startsWith(token, index)) {
            return false;
        }
        int beforeIndex = index - 1;
        int afterIndex = index + token.length();
        return (beforeIndex < 0 || isTokenBoundary(input.charAt(beforeIndex)))
                && (afterIndex >= input.length() || isTokenBoundary(input.charAt(afterIndex)));
    }

    private static boolean isTokenBoundary(char value) {
        return !Character.isLetterOrDigit(value) && value != '_' && value != '$';
    }

    private static String summarizeException(Exception exception) {
        if (exception == null) {
            return "";
        }
        String message = exception.getMessage();
        if (message == null || message.isEmpty()) {
            return exception.getClass().getSimpleName();
        }
        return message.replace('\n', ' ').replace('\r', ' ');
    }

    private static String previewJson(String jsonStr) {
        if (jsonStr == null) {
            return "";
        }
        String preview = jsonStr.replaceAll("\\s+", " ").trim();
        if (preview.length() <= JSON_PREVIEW_LENGTH) {
            return preview;
        }
        return preview.substring(0, JSON_PREVIEW_LENGTH) + "...";
    }

    /**
     * 安全解析JSON，不抛出异常
     *
     * @param jsonStr JSON字符串
     * @return JsonNode对象，解析失败返回null
     */
    public static JsonNode parseJsonSafe(String jsonStr) {
        return parseJson(jsonStr);
    }

    /**
     * 根据路径收集叶子节点
     * 支持嵌套JSON和数组遍历
     *
     * @param node  根节点
     * @param paths 路径数组（如 ["data", "items", "name"]）
     * @param index 当前路径索引
     * @return 叶子节点列表
     */
    public static List<JsonNode> collectLeafNodes(JsonNode node, String[] paths, int index) {
        List<JsonNode> result = new ArrayList<>();
        collectLeafNodesInternal(node, paths, index, result);
        return result;
    }

    /**
     * 内部递归方法：收集叶子节点
     */
    private static void collectLeafNodesInternal(JsonNode node, String[] paths, int index, List<JsonNode> result) {
        if (node == null || index >= paths.length) {
            return;
        }
        String path = paths[index];
        boolean isLast = index == paths.length - 1;

        if (node.isArray()) {
            for (JsonNode item : node) {
                JsonNode next = item.get(path);
                if (next == null) {
                    continue;
                }
                if (isLast) {
                    collectLeafValues(next, result);
                } else {
                    collectLeafNodesInternal(next, paths, index + 1, result);
                }
            }
            return;
        }

        JsonNode next = node.get(path);
        if (next == null) {
            return;
        }
        if (isLast) {
            collectLeafValues(next, result);
        } else {
            collectLeafNodesInternal(next, paths, index + 1, result);
        }
    }

    /**
     * 收集叶子值，展平数组
     */
    private static void collectLeafValues(JsonNode node, List<JsonNode> result) {
        if (node == null) {
            return;
        }
        if (node.isArray()) {
            node.forEach(item -> collectLeafValues(item, result));
        } else {
            result.add(node);
        }
    }

    /**
     * 将叶子节点列表转换为字符串
     * 单个值直接返回，多个值返回JSON数组格式
     *
     * @param leaves 叶子节点列表
     * @return 字符串结果
     */
    public static String leavesToString(List<JsonNode> leaves) {
        if (leaves == null || leaves.isEmpty()) {
            return null;
        }
        if (leaves.size() == 1 && leaves.get(0).isValueNode()) {
            JsonNode node = leaves.get(0);
            return node.isTextual() ? node.asText() : node.toString();
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < leaves.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            JsonNode node = leaves.get(i);
            sb.append(node.isTextual() ? "\"" + node.asText() + "\"" : node.toString());
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * JsonNode 转字符串，处理各种类型
     *
     * @param node JsonNode节点
     * @return 字符串表示
     */
    public static String nodeToString(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
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
        // 对象或数组返回JSON字符串
        return node.toString();
    }

    /**
     * 安全获取节点的文本值
     *
     * @param node      父节点
     * @param fieldName 字段名
     * @return 文本值，不存在返回null
     */
    public static String getTextValue(JsonNode node, String fieldName) {
        if (node == null || !node.has(fieldName)) {
            return null;
        }
        JsonNode fieldNode = node.get(fieldName);
        return nodeToString(fieldNode);
    }

    /**
     * 对象转JSON字符串
     *
     * @param obj 对象
     * @return JSON字符串
     */
    public static String toJsonString(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            LOGGER.error("对象转JSON失败: {}", e.getMessage());
            return null;
        }
    }
}

