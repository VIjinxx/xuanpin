package com.lingyun.business.common.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.lingyun.business.common.util.PropertiesUtil;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Doris批量写入Sink
 * 使用Doris Flink Connector进行数据写入
 * 支持泛型，可处理不同类型的 Record
 *
 * @author wxx
 */
public class DorisBatchSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(DorisBatchSink.class);

    /** 日志输出间隔，1 表示每条记录都打印 */
    private static final long LOG_SAMPLE_INTERVAL = Long.parseLong(
            PropertiesUtil.getProperty("log.sample.interval", "1"));

    /** 线程安全的ObjectMapper，用于JSON序列化 */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** 各表的计数器缓存 */
    private static final ConcurrentHashMap<String, AtomicLong> TABLE_COUNTERS = new ConcurrentHashMap<>();
    private static final int LABEL_PREFIX_MAX_LENGTH = 120;
    private static final String RUN_LABEL_TOKEN =
            UUID.randomUUID().toString().replace("-", "").substring(0, 12);

    // Doris连接配置
    private static final String DORIS_FE_NODES = PropertiesUtil.getProperty("doris.fe.nodes", "hdp01:8030");
    private static final String DORIS_DATABASE = PropertiesUtil.getProperty("doris.database", "lingyun");
    private static final String DORIS_USER = PropertiesUtil.getProperty("doris.user", "root");
    private static final String DORIS_PASSWORD = PropertiesUtil.getProperty("doris.password", "root");

    /**
     * 创建Doris基础配置
     */
    private static DorisOptions createDorisOptions(String tableName) {
        return DorisOptions.builder()
                .setFenodes(DORIS_FE_NODES)
                .setTableIdentifier(DORIS_DATABASE + "." + tableName)
                .setUsername(DORIS_USER)
                .setPassword(DORIS_PASSWORD)
                .build();
    }

    /**
     * 创建Doris执行配置
     * 生产环境建议：BufferSize 调整为 1MB~5MB，BufferCount 调整为 3~5
     */
    static DorisExecutionOptions createExecutionOptions(String tableName) {
        int bufferSizeKb = Integer.parseInt(
                PropertiesUtil.getProperty("doris.buffer.size.kb", "1024"));
        int bufferCount = Integer.parseInt(
                PropertiesUtil.getProperty("doris.buffer.count", "3"));
        String labelPrefix = createLabelPrefix(tableName);
        boolean localMode = Boolean.parseBoolean(System.getProperty("lingyun.job.local", "false"));

        LOGGER.info("创建Doris执行配置: FE={}, DB={}, User={}, Table={}, BufferSize={}KB, BufferCount={}, LabelPrefix={}",
                DORIS_FE_NODES, DORIS_DATABASE, DORIS_USER, tableName, bufferSizeKb, bufferCount, labelPrefix);

        DorisExecutionOptions.Builder builder = DorisExecutionOptions.builder()
                .setBufferSize(bufferSizeKb * 1024)
                .setBufferCount(bufferCount)
                .setMaxRetries(3)
                .setStreamLoadProp(getStreamLoadProperties())
                .setDeletable(false)
                .setLabelPrefix(labelPrefix);

        if (localMode) {
            LOGGER.info("本地模式 Doris Sink 配置: 保留 Streaming 写入模式和 2PC，等待 checkpoint 完成后提交可见");
        }

        return builder.build();
    }

    /**
     * 获取Stream Load属性配置
     */
    private static Properties getStreamLoadProperties() {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        props.setProperty("partial_columns", PropertiesUtil.getProperty("doris.partial.columns", "false"));
        props.setProperty("strict_mode", PropertiesUtil.getProperty("doris.strict.mode", "true"));
        LOGGER.info("Stream Load配置: {}", props);
        return props;
    }

    static String createLabelPrefix(String tableName) {
        String configuredPrefix = firstNotBlank(
                System.getProperty("doris.label.prefix"),
                System.getenv("DORIS_LABEL_PREFIX"),
                PropertiesUtil.getProperty("doris.label.prefix"));
        return createLabelPrefix(tableName, configuredPrefix, RUN_LABEL_TOKEN);
    }

    static String createLabelPrefix(String tableName, String configuredPrefix, String runLabelToken) {
        String labelRoot = configuredPrefix == null
                ? "lf_" + abbreviateLabelPart(runLabelToken)
                : abbreviateLabelPart(configuredPrefix);
        String rawPrefix = labelRoot + "_" + createShortTableLabel(tableName);
        String sanitized = sanitizeLabelPrefix(rawPrefix);
        if (sanitized.length() <= LABEL_PREFIX_MAX_LENGTH) {
            return sanitized;
        }

        String hash = Integer.toHexString(sanitized.hashCode());
        String suffix = createShortTableLabel(tableName);
        String fallback = "lf_" + hash + "_" + suffix;
        if (fallback.length() <= LABEL_PREFIX_MAX_LENGTH) {
            return fallback;
        }
        return fallback.substring(0, LABEL_PREFIX_MAX_LENGTH);
    }

    private static String createShortTableLabel(String tableName) {
        String sanitized = sanitizeLabelPrefix(tableName);
        String[] parts = sanitized.split("_");
        if (parts.length >= 3) {
            String layer = parts[0];
            String suffix = parts[parts.length - 1];
            if (("ods".equals(layer) && "raw".equals(suffix))
                    || ("dwd".equals(layer) && "base".equals(suffix))) {
                StringBuilder subject = new StringBuilder();
                for (int i = 1; i < parts.length - 1; i++) {
                    if (subject.length() > 0) {
                        subject.append('_');
                    }
                    subject.append(parts[i]);
                }
                return abbreviateLabelPart(subject.toString()) + "_" + layer;
            }
        }
        return abbreviateLabelPart(sanitized);
    }

    private static String abbreviateLabelPart(String value) {
        String sanitized = sanitizeLabelPrefix(value);
        String[] parts = sanitized.split("_");
        StringBuilder abbreviated = new StringBuilder(sanitized.length());
        for (String part : parts) {
            if (part.isEmpty()) {
                continue;
            }
            if (abbreviated.length() > 0) {
                abbreviated.append('_');
            }
            abbreviated.append(abbreviateLabelToken(part));
        }
        return abbreviated.length() == 0 ? "unknown" : abbreviated.toString();
    }

    private static String abbreviateLabelToken(String token) {
        String normalized = token.toLowerCase(Locale.ROOT);
        if ("lingfeng".equals(normalized)) {
            return "lf";
        }
        if ("category".equals(normalized) || "categories".equals(normalized)) {
            return "cat";
        }
        if ("malls".equals(normalized)) {
            return "mall";
        }
        return token;
    }

    private static String sanitizeLabelPrefix(String value) {
        if (value == null || value.isEmpty()) {
            return "unknown";
        }

        StringBuilder result = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') {
                result.append(c);
            } else {
                result.append('_');
            }
        }
        return result.toString();
    }

    private static String firstNotBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                return value.trim();
            }
        }
        return null;
    }

    /**
     * 创建ODS层Doris Sink（使用枚举配置）
     *
     * @param tableConfig 表配置枚举
     * @return DorisSink实例
     */
    public static DorisSink<String> createODSDorisSink(DorisTableConfig tableConfig) {
        return createODSDorisSink(tableConfig.getTableName());
    }

    /**
     * 创建DWD层Doris Sink（使用枚举配置）
     *
     * @param tableConfig 表配置枚举
     * @return DorisSink实例
     */
    public static DorisSink<String> createDWDDorisSink(DorisTableConfig tableConfig) {
        return createDWDDorisSink(tableConfig.getTableName());
    }

    /**
     * 获取指定表的计数器
     *
     * @param tableName 表名
     * @return 计数器
     */
    public static AtomicLong getCounter(String tableName) {
        return TABLE_COUNTERS.computeIfAbsent(tableName, k -> new AtomicLong(0));
    }

    /**
     * 创建ODS层Doris Sink
     *
     * @param odsTableName ODS表名
     * @return DorisSink实例
     */
    public static DorisSink<String> createODSDorisSink(String odsTableName) {
        return createDorisSink(odsTableName, "ods");
    }

    /**
     * 创建DWD层Doris Sink
     * 
     * @param dwdTableName DWD表名
     * @return DorisSink实例
     */
    public static DorisSink<String> createDWDDorisSink(String dwdTableName) {
        return createDorisSink(dwdTableName, "dwd");
    }

    private static DorisSink<String> createDorisSink(String tableName, String layer) {
        return new DorisWriteSuccessLoggingSink(
                createDorisOptions(tableName),
                DorisReadOptions.builder().build(),
                createExecutionOptions(tableName),
                new SimpleStringSerializer(),
                layer,
                tableName);
    }

    /**
     * 将Record转换为JSON字符串（使用ObjectMapper转换）
     * 使用共享的ObjectMapper实例，线程安全
     *
     * @param record 记录对象
     * @param recordClass 记录类型
     * @param counter 计数器（用于采样日志）
     * @param logFieldsExtractor 日志字段提取器（用于日志输出）
     * @param <T> 记录类型
     * @return JSON字符串
     */
    public static <T> String recordToJson(T record, Class<T> recordClass, AtomicLong counter, Function<T, String> logFieldsExtractor) {
        if (record == null) {
            LOGGER.debug("【转JSON】传入的record为null");
            return null;
        }
        try {
            return recordToJsonByReflection(record, recordClass, counter, logFieldsExtractor);
        } catch (Exception e) {
            LOGGER.error("Record转JSON失败: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * 将Record转换为JSON字符串（使用反射获取字段）
     * 使用共享的ObjectMapper实例，线程安全
     *
     * @param record 记录对象
     * @param recordClass 记录类型
     * @param counter 计数器（用于采样日志）
     * @param logFieldsExtractor 日志字段提取器（用于日志输出）
     * @param <T> 记录类型
     * @return JSON字符串
     */
    public static <T> String recordToJsonByReflection(T record, Class<T> recordClass, AtomicLong counter, Function<T, String> logFieldsExtractor) {
        return recordToJsonByReflection(record, recordClass, counter, logFieldsExtractor,
                Collections.emptySet(), null);
    }

    public static <T> String recordToJsonByReflection(T record, Class<T> recordClass, AtomicLong counter,
                                                      Function<T, String> logFieldsExtractor,
                                                      Set<String> excludedFields,
                                                      BiFunction<String, Object, Object> valueProcessor) {
        if (record == null) {
            LOGGER.debug("【转JSON】传入的record为null");
            return null;
        }
        try {
            Field[] fields = recordClass.getDeclaredFields();
            Map<String, Object> jsonMap = new LinkedHashMap<>(fields.length + 1);

            for (Field field : fields) {
                field.setAccessible(true);
                if (excludedFields != null && excludedFields.contains(field.getName())) {
                    continue;
                }
                Object value = field.get(record);
                if (valueProcessor != null) {
                    value = valueProcessor.apply(field.getName(), value);
                }
                jsonMap.put(field.getName(), processValue(value, recordClass));
            }

            String json = OBJECT_MAPPER.writeValueAsString(jsonMap);
            if (json == null || json.isEmpty() || "{}".equals(json)) {
                LOGGER.warn("【转JSON】生成的JSON为空: {}", json);
                return null;
            }

            // 采样日志
            logSample(counter, logFieldsExtractor, record);
            return json;
        } catch (Exception e) {
            LOGGER.error("Record转JSON失败: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * 采样日志输出
     */
    private static <T> void logSample(AtomicLong counter, Function<T, String> logFieldsExtractor, T record) {
        if (counter != null) {
            long count = counter.incrementAndGet();
            if (count % LOG_SAMPLE_INTERVAL == 0 && logFieldsExtractor != null) {
                LOGGER.info("【写入进度】已处理 {} 条, 最近一条: {}",
                        count, logFieldsExtractor.apply(record));
            }
        }
    }

    /**
     * 处理值，将复杂类型转换为JSON字符串
     */
    private static <T> Object processValue(Object value, Class<T> recordClass) {
        if (value == null) {
            return null;
        }

        // 字符串直接返回
        if (value instanceof String) {
            return value;
        }

        if (value instanceof Number || value instanceof Boolean) {
            return value;
        }

        if (value instanceof JsonNode) {
            return value;
        }

        if (value instanceof LocalDate) {
            return value.toString();
        }

        // 集合类型序列化为JSON
        if (value instanceof List || value instanceof Map) {
            try {
                return OBJECT_MAPPER.writeValueAsString(value);
            } catch (Exception e) {
                LOGGER.warn("序列化集合失败: {}", e.getMessage());
                return null;
            }
        }

        // 内部类对象序列化为JSON（通过包名判断）
        String className = value.getClass().getName();
        String recordPackage = recordClass.getPackage().getName();
        if (className.startsWith(recordPackage + "." + recordClass.getSimpleName() + "$")) {
            try {
                return OBJECT_MAPPER.writeValueAsString(value);
            } catch (Exception e) {
                LOGGER.warn("序列化内部类失败: {}", e.getMessage());
                return null;
            }
        }

        // 其他类型转字符串
        return value.toString();
    }
}

