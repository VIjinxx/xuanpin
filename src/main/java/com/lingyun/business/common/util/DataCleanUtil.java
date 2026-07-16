package com.lingyun.business.common.util;

import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 数据清洗工具类
 * 提供通用的数据格式转换方法
 *
 * @author wxx
 */
public class DataCleanUtil {
    private static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+(?:\\.\\d+)?");

    private DataCleanUtil() {
        // 工具类禁止实例化
    }

    /**
     * 将字符串销量转换为数值字符串
     * 支持处理：
     * - 带"万"的情况（如"8.1万" -> "81000"）
     * - 带"K+"的情况（如"70K+" -> "70000"）
     * - 带逗号的情况（如"3,145" -> "3145"）
     *
     * @param salesStr 销量字符串
     * @return 转换后的数值字符串，无效输入返回null
     */
    public static String parseSalesNum(String salesStr) {
        if (salesStr == null || salesStr.trim().isEmpty()) {
            return null;
        }
        salesStr = salesStr.trim();
        if ("null".equalsIgnoreCase(salesStr)) {
            return null;
        }

        double multiplier = 1D;
        String upper = salesStr.toUpperCase();
        if (upper.contains("万")) {
            multiplier = 10000D;
        } else if (upper.contains("K")) {
            multiplier = 1000D;
        } else if (upper.contains("M")) {
            multiplier = 1000000D;
        }

        Matcher matcher = NUMBER_PATTERN.matcher(salesStr.replace(",", ""));
        if (!matcher.find()) {
            return null;
        }

        try {
            long parsed = Math.round(Double.parseDouble(matcher.group()) * multiplier);
            return String.valueOf(parsed);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Extract a decimal number from display text while preserving the fraction.
     */
    public static String parseDecimal(String decimalStr) {
        if (decimalStr == null || decimalStr.trim().isEmpty()) {
            return null;
        }
        decimalStr = decimalStr.trim();
        if ("null".equalsIgnoreCase(decimalStr)) {
            return null;
        }

        Matcher matcher = NUMBER_PATTERN.matcher(decimalStr.replace(",", ""));
        if (!matcher.find()) {
            return null;
        }
        try {
            return new BigDecimal(matcher.group()).stripTrailingZeros().toPlainString();
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 优先解析销量字段；为空时从销量单位数组的第一个值中解析。
     *
     * @param salesNum 销量字段
     * @param salesNumUnit 销量单位数组，例如 ["4.8万","已售"] 或 ["70K+","Sold"]
     * @return 转换后的数值字符串，无效输入返回null
     */
    public static String parseSalesNumWithUnitFallback(String salesNum, String salesNumUnit) {
        String parsed = parseSalesNum(salesNum);
        if (parsed != null) {
            return parsed;
        }

        return parseSalesNum(extractFirstArrayValue(salesNumUnit));
    }

    private static String extractFirstArrayValue(String jsonArrayStr) {
        JsonNode rootNode = JsonUtil.parseJson(jsonArrayStr);
        if (rootNode == null || !rootNode.isArray() || rootNode.isEmpty()) {
            return null;
        }
        return JsonUtil.nodeToString(rootNode.get(0));
    }

    /**
     * 驼峰命名转下划线命名
     * 例如：optId -> opt_id, userName -> user_name
     *
     * @param camelCase 驼峰格式字符串
     * @return 下划线格式字符串
     */
    public static String camelToSnake(String camelCase) {
        if (camelCase == null || camelCase.isEmpty()) {
            return camelCase;
        }
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < camelCase.length(); i++) {
            char c = camelCase.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) {
                    result.append('_');
                }
                result.append(Character.toLowerCase(c));
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    /**
     * 下划线命名转驼峰命名
     * 例如：opt_id -> optId, user_name -> userName
     *
     * @param snakeCase 下划线格式字符串
     * @return 驼峰格式字符串
     */
    public static String snakeToCamel(String snakeCase) {
        if (snakeCase == null || snakeCase.isEmpty()) {
            return snakeCase;
        }
        StringBuilder result = new StringBuilder();
        boolean nextUpperCase = false;
        for (int i = 0; i < snakeCase.length(); i++) {
            char c = snakeCase.charAt(i);
            if (c == '_') {
                nextUpperCase = true;
            } else {
                if (nextUpperCase) {
                    result.append(Character.toUpperCase(c));
                    nextUpperCase = false;
                } else {
                    result.append(c);
                }
            }
        }
        return result.toString();
    }

    /**
     * 检查字符串是否为空或null
     *
     * @param str 待检查字符串
     * @return true表示为空或null
     */
    public static boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    /**
     * 检查字符串是否非空且非null
     *
     * @param str 待检查字符串
     * @return true表示非空且非null
     */
    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    /**
     * 安全的字符串trim，null安全
     *
     * @param str 待处理字符串
     * @return trim后的字符串，null返回null
     */
    public static String safeTrim(String str) {
        return str == null ? null : str.trim();
    }
}
