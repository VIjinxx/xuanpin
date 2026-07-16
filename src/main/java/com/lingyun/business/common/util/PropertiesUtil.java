package com.lingyun.business.common.util;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {
    private static final Properties properties = new Properties();
    static {
        try (InputStream input = PropertiesUtil.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("无法找到 application.properties 文件");
            }
            properties.load(input);
        } catch (IOException ex) {
            throw new RuntimeException("加载配置文件失败", ex);
        }
    }
    /**
     * 配置 Checkpoint 参数和任务并行度
     */
    public static void configureCheckpoint(StreamExecutionEnvironment env) {
        // 从配置文件读取 checkpoint 参数
        long checkpointInterval = Long.parseLong(
                PropertiesUtil.getProperty("flink.checkpoint.interval.ms", "60000"));
        long checkpointTimeout = Long.parseLong(
                PropertiesUtil.getProperty("flink.checkpoint.timeout.ms", "120000"));
        long minPauseBetweenCheckpoints = Long.parseLong(
                PropertiesUtil.getProperty("flink.checkpoint.min.pause.ms", "30000"));
        int maxConcurrentCheckpoints = Integer.parseInt(
                PropertiesUtil.getProperty("flink.checkpoint.max.concurrent", "1"));
        int tolerableFailureNumber = Integer.parseInt(
                PropertiesUtil.getProperty("flink.checkpoint.tolerable.failure.number", "3"));
        String checkpointModeStr = PropertiesUtil.getProperty("flink.checkpoint.mode", "EXACTLY_ONCE");

        // 解析 checkpoint 模式
        CheckpointingMode checkpointingMode = "AT_LEAST_ONCE".equalsIgnoreCase(checkpointModeStr)
                ? CheckpointingMode.AT_LEAST_ONCE
                : CheckpointingMode.EXACTLY_ONCE;

        // 开启 Checkpoint
        env.enableCheckpointing(checkpointInterval, checkpointingMode);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // Checkpoint 超时时间
        checkpointConfig.setCheckpointTimeout(checkpointTimeout);

        // 两次 Checkpoint 之间的最小间隔，防止 Checkpoint 过于频繁
        checkpointConfig.setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);

        // 允许同时进行的 Checkpoint 数量
        checkpointConfig.setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);

        // 任务取消时保留 Checkpoint（用于手动恢复）
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 容忍 Checkpoint 失败次数（避免单次失败导致任务挂掉）
        checkpointConfig.setTolerableCheckpointFailureNumber(tolerableFailureNumber);
    }
    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}

