package com.lingyun.business.common.util;

import com.lingyun.business.common.dao.FlinkKafkaDao;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 创建任务数据源，默认 Kafka，仅在 --local true 时读取本地样例文件。
 */
public final class JobSourceFactory {
    public static final String LOCAL_MODE_PROPERTY = "lingyun.job.local";

    private static final Logger LOGGER = LoggerFactory.getLogger(JobSourceFactory.class);
    private static final String LOCAL_ARG = "--local";
    private static final String FILE_ARG = "--file";

    private JobSourceFactory() {
    }

    public static DataStream<String> createSourceStream(
            StreamExecutionEnvironment env,
            String[] args,
            String topic,
            String groupId,
            String dataLabel,
            String defaultSamplePath) {

        LOGGER.info("任务启动参数: {}", Arrays.toString(args == null ? new String[0] : args));
        SourceOptions options = resolveOptions(args, defaultSamplePath);
        if (options.isLocal()) {
            try {
                System.setProperty(LOCAL_MODE_PROPERTY, "true");
                String jsonContent = JsonFileSource.readJsonContent(options.getSourcePath());
                if (jsonContent.isEmpty()) {
                    throw new IllegalArgumentException("本地样例文件内容为空: " + options.getSourcePath());
                }
                long checkpointWaitTimeoutMs = resolveLocalCheckpointWaitTimeoutMs(env);
                LOGGER.info("使用本地样例文件数据源: {}，以单条有限数据运行，保留当前Checkpoint配置，checkpoint等待超时={}ms",
                        options.getSourcePath(), checkpointWaitTimeoutMs);
                return env
                        .addSource(new JsonFileSource(options.getSourcePath(), checkpointWaitTimeoutMs))
                        .name("File_Source")
                        .uid("file-source");
            } catch (Exception e) {
                throw new RuntimeException("创建本地样例文件数据源失败: " + options.getSourcePath(), e);
            }
        }

        System.setProperty(LOCAL_MODE_PROPERTY, "false");
        if (options.hasFileOption()) {
            LOGGER.warn("检测到 --file 参数但未启用 --local true，仍使用 Kafka 数据源: {}",
                    options.getSourcePath());
        }

        LOGGER.info("使用 Kafka 数据源: topic={}, groupId={}", topic, groupId);
        KafkaSource<String> kafkaSource = new FlinkKafkaDao(topic, groupId).createSource();
        return env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source")
                .uid("kafka-source")
                .map(new DataLogMapFunction(dataLabel))
                .name("Kafka_Log")
                .uid("kafka-log");
    }

    public static SourceOptions resolveOptions(String[] args, String defaultSamplePath) {
        boolean local = false;
        boolean fileOption = false;
        String filePath = null;
        String[] safeArgs = normalizeArgs(args);

        for (int i = 0; i < safeArgs.length; i++) {
            String arg = safeArgs[i];
            if (arg == null) {
                continue;
            }

            if (LOCAL_ARG.equals(arg)) {
                if (i + 1 < safeArgs.length && !safeArgs[i + 1].startsWith("--")) {
                    local = parseBooleanArg(LOCAL_ARG, safeArgs[++i]);
                } else {
                    local = true;
                }
            } else if (arg.startsWith(LOCAL_ARG + "=")) {
                local = parseBooleanArg(LOCAL_ARG, arg.substring((LOCAL_ARG + "=").length()));
            } else if (FILE_ARG.equals(arg)) {
                fileOption = true;
                if (i + 1 >= safeArgs.length) {
                    throw new IllegalArgumentException("--file 参数后必须提供文件路径");
                }
                filePath = safeArgs[++i];
            } else if (arg.startsWith(FILE_ARG + "=")) {
                fileOption = true;
                filePath = arg.substring((FILE_ARG + "=").length());
            }
        }

        String sourcePath = fileOption ? filePath : defaultSamplePath;
        return new SourceOptions(local, fileOption, sourcePath);
    }

    static long resolveLocalCheckpointWaitTimeoutMs(StreamExecutionEnvironment env) {
        String configured = PropertiesUtil.getProperty("flink.local.source.checkpoint.wait.timeout.ms");
        if (configured != null && !configured.trim().isEmpty()) {
            return Long.parseLong(configured.trim());
        }

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        if (!checkpointConfig.isCheckpointingEnabled()) {
            return 0L;
        }

        long interval = checkpointConfig.getCheckpointInterval();
        long timeout = checkpointConfig.getCheckpointTimeout();
        long minPause = checkpointConfig.getMinPauseBetweenCheckpoints();
        return Math.max(interval + timeout + minPause, interval + 1000L);
    }

    private static boolean isTruthy(String value) {
        if (value == null) {
            return false;
        }
        String normalized = value.trim();
        return "true".equalsIgnoreCase(normalized)
                || "1".equals(normalized)
                || "yes".equalsIgnoreCase(normalized)
                || "on".equalsIgnoreCase(normalized);
    }

    private static boolean parseBooleanArg(String argName, String value) {
        if (isTruthy(value)) {
            return true;
        }
        if (isFalsy(value)) {
            return false;
        }
        throw new IllegalArgumentException(argName + " 参数值无效: " + value
                + "，请使用 true/false、1/0、yes/no 或 on/off");
    }

    private static boolean isFalsy(String value) {
        if (value == null) {
            return false;
        }
        String normalized = value.trim();
        return "false".equalsIgnoreCase(normalized)
                || "0".equals(normalized)
                || "no".equalsIgnoreCase(normalized)
                || "off".equalsIgnoreCase(normalized);
    }

    private static String[] normalizeArgs(String[] args) {
        if (args == null || args.length == 0) {
            return new String[0];
        }

        List<String> normalized = new ArrayList<>();
        for (String arg : args) {
            if (arg == null) {
                continue;
            }

            String trimmed = arg.trim();
            if (trimmed.isEmpty()) {
                continue;
            }

            if (trimmed.indexOf(' ') >= 0 || trimmed.indexOf('\t') >= 0) {
                normalized.addAll(Arrays.asList(trimmed.split("\\s+")));
            } else {
                normalized.add(trimmed);
            }
        }
        return normalized.toArray(new String[0]);
    }

    public static final class SourceOptions {
        private final boolean local;
        private final boolean fileOption;
        private final String sourcePath;

        private SourceOptions(boolean local, boolean fileOption, String sourcePath) {
            this.local = local;
            this.fileOption = fileOption;
            this.sourcePath = sourcePath;
        }

        public boolean isLocal() {
            return local;
        }

        public boolean hasFileOption() {
            return fileOption;
        }

        public String getSourcePath() {
            return sourcePath;
        }
    }
}
