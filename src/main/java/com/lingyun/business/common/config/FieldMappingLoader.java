package com.lingyun.business.common.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 字段映射配置加载器
 * 支持加载不同的配置文件
 */
public class FieldMappingLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(FieldMappingLoader.class);
    
    /** 缓存已加载的配置，避免重复加载 */
    private static final Map<String, Map<String, String>> configCache = new ConcurrentHashMap<>();
    private static final Map<String, Map<String, List<String>>> sourceConfigCache = new ConcurrentHashMap<>();

    /**
     * 获取字段配置
     * @param configFile 配置文件名
     * @return 字段映射配置
     */
    public static Map<String, String> getFieldMappings(String configFile) {
        return configCache.computeIfAbsent(configFile, FieldMappingLoader::loadConfig);
    }

    /**
     * 获取字段多来源配置。
     * JSON 中的字符串会被视为单来源，数组会按下标保留为多个来源。
     */
    public static Map<String, List<String>> getFieldMappingSources(String configFile) {
        return sourceConfigCache.computeIfAbsent(configFile, FieldMappingLoader::loadSourceConfig);
    }

    /**
     * 加载配置文件
     * 使用 try-with-resources 确保 InputStream 正确关闭
     */
    private static Map<String, String> loadConfig(String configFile) {
        Map<String, List<String>> sourceMappings = getFieldMappingSources(configFile);
        Map<String, String> fieldMappings = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : sourceMappings.entrySet()) {
            fieldMappings.put(entry.getKey(), firstNonEmptySource(entry.getValue()));
        }
        LOGGER.info("成功加载字段映射配置 {}，共 {} 个映射", configFile, fieldMappings.size());
        return fieldMappings;
    }

    private static Map<String, List<String>> loadSourceConfig(String configFile) {
        // 每次创建新的 ObjectMapper 实例，避免类加载器泄漏
        ObjectMapper objectMapper = new ObjectMapper();
        try (InputStream is = FieldMappingLoader.class.getClassLoader()
                .getResourceAsStream(configFile)) {
            if (is == null) {
                throw new RuntimeException("无法找到配置文件: " + configFile);
            }
            Map<String, JsonNode> rawMappings = objectMapper.readValue(
                    is,
                    new TypeReference<LinkedHashMap<String, JsonNode>>() {}
            );
            Map<String, List<String>> fieldMappings = new LinkedHashMap<>();
            for (Map.Entry<String, JsonNode> entry : rawMappings.entrySet()) {
                fieldMappings.put(entry.getKey(), toSources(entry.getValue()));
            }
            LOGGER.info("成功加载字段多来源映射配置 {}，共 {} 个映射", configFile, fieldMappings.size());
            return fieldMappings;
        } catch (IOException e) {
            throw new RuntimeException("加载字段映射配置失败: " + e.getMessage(), e);
        }
    }

    private static List<String> toSources(JsonNode node) {
        List<String> sources = new ArrayList<>();
        if (node == null || node.isNull()) {
            sources.add("");
            return sources;
        }

        if (node.isArray()) {
            for (JsonNode item : node) {
                sources.add(toSourceText(item));
            }
            return sources;
        }

        sources.add(toSourceText(node));
        return sources;
    }

    private static String toSourceText(JsonNode node) {
        if (node == null || node.isNull()) {
            return "";
        }
        return node.isTextual() ? node.asText() : node.asText("");
    }

    private static String firstNonEmptySource(List<String> sources) {
        if (sources == null || sources.isEmpty()) {
            return "";
        }
        for (String source : sources) {
            if (source != null && !source.isEmpty()) {
                return source;
            }
        }
        return "";
    }
}

