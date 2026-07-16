package com.lingyun.business.common.util;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;

/**
 * 读取完整 JSON 文件的数据源
 * 将整个文件内容作为一个 JSON 字符串读取，而不是按行分割
 *
 * @author wxx
 */
public class JsonFileSource extends RichSourceFunction<String> implements CheckpointListener {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonFileSource.class);

    private final String resourcePath;
    private final long checkpointWaitTimeoutMs;
    private volatile boolean isRunning = true;
    private volatile boolean checkpointCompleted = false;

    public JsonFileSource(String resourcePath) {
        this(resourcePath, 0L);
    }

    public JsonFileSource(String resourcePath, long checkpointWaitTimeoutMs) {
        this.resourcePath = resourcePath;
        this.checkpointWaitTimeoutMs = Math.max(0L, checkpointWaitTimeoutMs);
    }

    public static String readJsonContent(String resourcePath) throws Exception {
        try (InputStream is = openInputStream(resourcePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder jsonBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                jsonBuilder.append(line).append("\n");
            }
            return jsonBuilder.toString().trim();
        }
    }

    InputStream openInputStream() throws Exception {
        return openInputStream(resourcePath);
    }

    private static InputStream openInputStream(String resourcePath) throws Exception {
        try {
            Path filePath = Paths.get(resourcePath);
            if (Files.isRegularFile(filePath)) {
                LOGGER.info("从文件系统读取 JSON 文件: {}", filePath.toAbsolutePath());
                return Files.newInputStream(filePath);
            }
        } catch (InvalidPathException e) {
            LOGGER.debug("不是有效的文件系统路径，尝试按 classpath resource 读取: {}", resourcePath);
        }

        InputStream inputStream = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(resourcePath);
        if (inputStream == null) {
            throw new RuntimeException("无法找到 JSON 文件: " + resourcePath);
        }
        LOGGER.info("从 classpath resource 读取 JSON 文件: {}", resourcePath);
        return inputStream;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        LOGGER.info("开始读取 JSON 文件: {}", resourcePath);

        try {
            String jsonContent = readJsonContent(resourcePath);
            LOGGER.info("成功读取 JSON 文件，内容长度: {}", jsonContent.length());

            if (isRunning && !jsonContent.isEmpty()) {
                ctx.collect(jsonContent);
                LOGGER.info("已发送完整 JSON 内容到数据流");
                waitForCheckpointIfNeeded();
            }
        } catch (Exception e) {
            LOGGER.error("读取 JSON 文件失败: {}", resourcePath, e);
            throw e;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        LOGGER.info("取消 JSON 文件读取任务");
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        checkpointCompleted = true;
        LOGGER.info("本地 JSON 文件源已收到 checkpoint 完成通知: checkpointId={}", checkpointId);
    }

    private void waitForCheckpointIfNeeded() throws InterruptedException {
        if (checkpointWaitTimeoutMs <= 0L) {
            return;
        }

        long deadline = System.currentTimeMillis() + checkpointWaitTimeoutMs;
        LOGGER.info("本地 JSON 文件源等待 checkpoint 完成，最长等待 {} ms", checkpointWaitTimeoutMs);
        while (isRunning && !checkpointCompleted) {
            long remainingMs = deadline - System.currentTimeMillis();
            if (remainingMs <= 0L) {
                break;
            }
            Thread.sleep(Math.min(200L, remainingMs));
        }

        if (checkpointCompleted) {
            LOGGER.info("本地 JSON 文件源检测到 checkpoint 已完成，允许有限样例任务结束");
        } else if (isRunning) {
            LOGGER.warn("本地 JSON 文件源等待 checkpoint 超时，任务将继续结束，Doris 2PC 事务可能无法提交可见");
        }
    }
}
