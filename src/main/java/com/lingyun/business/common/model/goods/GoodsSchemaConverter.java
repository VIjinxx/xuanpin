package com.lingyun.business.common.model.goods;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.lingyun.business.common.util.JsonUtil;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 商品 schema 类型转换器。
 */
public final class GoodsSchemaConverter {
    private static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+(?:\\.\\d+)?");
    private static final Pattern DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private GoodsSchemaConverter() {
    }

    public static Object convertJsonNode(JsonNode node, GoodsFieldType type) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        switch (type) {
            case STRING:
                return JsonUtil.nodeToString(node);
            case INTEGER:
                return toLong(node);
            case FLOAT:
                return toDouble(node);
            case BOOLEAN:
                return toBoolean(node);
            case DATE:
                return toDate(node);
            case JSON:
            default:
                return toJsonNode(node);
        }
    }

    public static Object convertObject(Object value, GoodsFieldType type) {
        if (value == null) {
            return null;
        }
        if (value instanceof JsonNode) {
            return convertJsonNode((JsonNode) value, type);
        }
        switch (type) {
            case STRING:
                return String.valueOf(value);
            case INTEGER:
                return toLong(String.valueOf(value));
            case FLOAT:
                return toDouble(String.valueOf(value));
            case BOOLEAN:
                return toBoolean(String.valueOf(value));
            case DATE:
                return toDate(String.valueOf(value));
            case JSON:
            default:
                return toJsonNode(value);
        }
    }

    public static Object convertToParameterType(Object value, Class<?> parameterType) {
        if (value == null) {
            return null;
        }
        if (String.class.equals(parameterType)) {
            return value instanceof JsonNode ? JsonUtil.nodeToString((JsonNode) value) : String.valueOf(value);
        }
        if (Long.class.equals(parameterType) || Long.TYPE.equals(parameterType)) {
            return value instanceof JsonNode ? toLong((JsonNode) value) : toLong(String.valueOf(value));
        }
        if (Double.class.equals(parameterType) || Double.TYPE.equals(parameterType)) {
            return value instanceof JsonNode ? toDouble((JsonNode) value) : toDouble(String.valueOf(value));
        }
        if (Boolean.class.equals(parameterType) || Boolean.TYPE.equals(parameterType)) {
            return value instanceof JsonNode ? toBoolean((JsonNode) value) : toBoolean(String.valueOf(value));
        }
        if (LocalDate.class.equals(parameterType)) {
            return value instanceof JsonNode ? toDate((JsonNode) value) : toDate(String.valueOf(value));
        }
        if (JsonNode.class.equals(parameterType)) {
            return value instanceof JsonNode ? toJsonNode((JsonNode) value) : toJsonNode(value);
        }
        return value;
    }

    private static Long toLong(JsonNode node) {
        if (node.isIntegralNumber()) {
            return node.longValue();
        }
        if (node.isNumber()) {
            return Math.round(node.doubleValue());
        }
        return toLong(node.asText());
    }

    private static Long toLong(String raw) {
        if (raw == null) {
            return null;
        }
        String value = raw.trim();
        if (value.isEmpty()) {
            return null;
        }
        double multiplier = 1D;
        String upper = value.toUpperCase();
        if (upper.contains("万")) {
            multiplier = 10000D;
        } else if (upper.contains("K")) {
            multiplier = 1000D;
        } else if (upper.contains("M")) {
            multiplier = 1000000D;
        }
        Matcher matcher = NUMBER_PATTERN.matcher(value.replace(",", ""));
        if (!matcher.find()) {
            return null;
        }
        String number = matcher.group();
        try {
            if (multiplier == 1D && !number.contains(".")) {
                return Long.parseLong(number);
            }
            return Math.round(Double.parseDouble(number) * multiplier);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Double toDouble(JsonNode node) {
        if (node.isNumber()) {
            return node.doubleValue();
        }
        return toDouble(node.asText());
    }

    private static Double toDouble(String raw) {
        if (raw == null) {
            return null;
        }
        Matcher matcher = NUMBER_PATTERN.matcher(raw.replace(",", ""));
        if (!matcher.find()) {
            return null;
        }
        try {
            return Double.parseDouble(matcher.group());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Boolean toBoolean(JsonNode node) {
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isNumber()) {
            return node.intValue() != 0;
        }
        return toBoolean(node.asText());
    }

    private static Boolean toBoolean(String raw) {
        if (raw == null) {
            return null;
        }
        String value = raw.trim().toLowerCase();
        if ("true".equals(value) || "1".equals(value) || "yes".equals(value)) {
            return Boolean.TRUE;
        }
        if ("false".equals(value) || "0".equals(value) || "no".equals(value)) {
            return Boolean.FALSE;
        }
        return null;
    }

    private static LocalDate toDate(JsonNode node) {
        if (node.isTextual()) {
            return toDate(node.asText());
        }
        return null;
    }

    private static LocalDate toDate(String raw) {
        if (raw == null) {
            return null;
        }
        Matcher matcher = DATE_PATTERN.matcher(raw);
        if (!matcher.find()) {
            return null;
        }
        try {
            return LocalDate.parse(matcher.group(), DATE_FORMATTER);
        } catch (Exception e) {
            return null;
        }
    }

    private static JsonNode toJsonNode(JsonNode node) {
        if (node.isTextual()) {
            JsonNode parsed = JsonUtil.parseJson(node.asText());
            return parsed == null ? node : parsed;
        }
        return node;
    }

    private static JsonNode toJsonNode(Object value) {
        if (value instanceof JsonNode) {
            return toJsonNode((JsonNode) value);
        }
        JsonNode parsed = JsonUtil.parseJson(String.valueOf(value));
        return parsed == null ? TextNode.valueOf(String.valueOf(value)) : parsed;
    }
}
