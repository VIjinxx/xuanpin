package com.lingyun.business.common.tools;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Kafka多线程压测工具 (优化版)
 * 1. 共享KafkaProducer实例
 * 2. 使用字节数组预处理模板，避免重复字符串编码
 * 3. 异步监控统计
 *
 * @author wxx
 */
public class KafkaStressTestTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStressTestTool.class);

    /** 发送成功计数器 */
    private static final LongAdder successCount = new LongAdder();
    /** 发送失败计数器 */
    private static final LongAdder failCount = new LongAdder();

    /** 模板数据的静态部分（字节数组） */
    private static final List<byte[]> templateParts = new ArrayList<>();
    /** mallId替换位置的数量 */
    private static int mallIdCount = 0;
    /** 预估的消息总长度 */
    private static int estimatedMsgLength = 0;

    /** 预编译的正则表达式 */
    private static final Pattern MALLID_NUMBER_PATTERN = Pattern.compile("\"mallId\"\\s*:\\s*(\\d+)");
    private static final Pattern MALLID_STRING_PATTERN = Pattern.compile("\"mallId\"\\s*:\\s*\"(\\d+)\"");

    public static void main(String[] args) throws Exception {
        // 解析命令行参数
        Options options = new Options();
        options.addOption("b", "bootstrap-servers", true, "Kafka bootstrap servers (default: localhost:9092)");
        options.addOption("t", "topic", true, "Kafka topic (default: test)");
        options.addOption("n", "num-messages", true, "Total number of messages to send (default: 10000)");
        options.addOption("p", "parallelism", true, "Number of parallel threads (default: 4)");
        options.addOption("d", "data-file", true, "JSON data file path in resources (optional)");
        options.addOption("h", "help", false, "Show help");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            LOGGER.error("参数解析失败: {}", e.getMessage());
            printHelp(options);
            return;
        }

        if (cmd.hasOption("h")) {
            printHelp(options);
            return;
        }

        // 获取参数
        String bootstrapServers = cmd.getOptionValue("b", "hdp01:9092");
        String topic = cmd.getOptionValue("t", "test");
        int numMessages = Integer.parseInt(cmd.getOptionValue("n", "10000"));
        int parallelism = Integer.parseInt(cmd.getOptionValue("p", "4"));
        String dataFile = cmd.getOptionValue("d", "samples/店铺详情页.json");

        LOGGER.info("========== Kafka压测工具启动 (优化版) ==========");
        LOGGER.info("Bootstrap Servers: {}", bootstrapServers);
        LOGGER.info("Topic: {}", topic);
        LOGGER.info("Total Messages: {}", numMessages);
        LOGGER.info("Parallelism: {}", parallelism);
        LOGGER.info("Data File: {}", dataFile);

        // 加载并预处理模板数据
        prepareTemplate(dataFile);

        // 初始化共享 Producer 配置
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()); // 使用字节数组Serializer

        // 性能优化配置
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072); // 128KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 134217728); // 128MB
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();

        // 启动监控线程
        long startTime = System.currentTimeMillis();
        monitor.scheduleAtFixedRate(() -> {
            long duration = System.currentTimeMillis() - startTime;
            long success = successCount.sum();
            long fail = failCount.sum();
            long throughput = (success * 1000) / Math.max(duration, 1);
            LOGGER.info("已发送: {} (成功: {}, 失败: {}), 耗时: {}s, 吞吐量: {} msg/s",
                    success + fail, success, fail, duration / 1000, throughput);
        }, 1, 1, TimeUnit.SECONDS);

        int messagesPerThread = numMessages / parallelism;
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < parallelism; i++) {
            final int threadId = i;
            final int count = (i == parallelism - 1) ? (numMessages - messagesPerThread * i) : messagesPerThread;
            futures.add(executor.submit(() -> sendMessages(producer, topic, count, threadId)));
        }

        // 等待所有任务完成
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                LOGGER.error("任务执行失败: {}", e.getMessage());
            }
        }

        monitor.shutdown();
        executor.shutdown();
        producer.close();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // 打印最终统计信息
        LOGGER.info("========== 压测完成 ==========");
        LOGGER.info("总耗时: {} ms", duration);
        LOGGER.info("成功发送: {} 条", successCount.sum());
        LOGGER.info("发送失败: {} 条", failCount.sum());
        LOGGER.info("平均吞吐量: {} msg/s", (successCount.sum() * 1000) / Math.max(duration, 1));
    }

    private static void sendMessages(KafkaProducer<String, byte[]> producer, String topic, int count, int threadId) {
        // 每个线程复用 StringBuilder 生成 Key，避免字符串拼接开销
        StringBuilder keyBuilder = new StringBuilder(32);
        String keyPrefix = "mall_" + threadId + "_";

        for (int i = 0; i < count; i++) {
            try {
                // 直接生成正确大小的字节数组，避免二次拷贝
                byte[] value = generateMessageBytes(threadId, i);

                // 复用 StringBuilder 生成 Key
                keyBuilder.setLength(0);
                keyBuilder.append(keyPrefix).append(i);

                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, keyBuilder.toString(), value);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        failCount.increment();
                    } else {
                        successCount.increment();
                    }
                });
            } catch (Exception e) {
                failCount.increment();
                LOGGER.debug("生成消息失败: {}", e.getMessage());
            }
        }
    }

    /**
     * 生成消息字节数组，直接返回正确大小的数组
     */
    private static byte[] generateMessageBytes(int threadId, int msgIndex) {
        // 生成随机ID (12位)
        long randomNum = ThreadLocalRandom.current().nextLong(100000000000L, 1000000000000L);
        byte[] idBytes = Long.toString(randomNum).getBytes(StandardCharsets.UTF_8);

        // 先写入临时缓冲区
        byte[] buffer = new byte[estimatedMsgLength + 50];
        int offset = 0;
        int partsSize = templateParts.size();

        for (int idx = 0; idx < partsSize; idx++) {
            // 复制静态部分
            byte[] part = templateParts.get(idx);
            System.arraycopy(part, 0, buffer, offset, part.length);
            offset += part.length;

            // 复制动态部分 (如果不是最后一段)
            // 注意：模板切分时已保留了原始的引号格式，这里只需复制数字即可
            if (idx < mallIdCount) {
                System.arraycopy(idBytes, 0, buffer, offset, idBytes.length);
                offset += idBytes.length;
            }
        }

        // 直接返回正确大小的数组（使用 Arrays.copyOf 比手动拷贝更高效）
        return java.util.Arrays.copyOf(buffer, offset);
    }

    private static void prepareTemplate(String dataFile) throws Exception {
        String templateString;
        try (InputStream is = KafkaStressTestTool.class.getClassLoader().getResourceAsStream(dataFile)) {
            if (is == null)
                throw new IllegalArgumentException("无法找到数据文件: " + dataFile);
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            templateString = result.toString(StandardCharsets.UTF_8.name());
        }

        // 查找所有替换位置
        List<MallIdPosition> positions = new ArrayList<>();
        Matcher numMatcher = MALLID_NUMBER_PATTERN.matcher(templateString);
        while (numMatcher.find())
            positions.add(new MallIdPosition(numMatcher.start(1), numMatcher.end(1)));

        Matcher strMatcher = MALLID_STRING_PATTERN.matcher(templateString);
        while (strMatcher.find())
            positions.add(new MallIdPosition(strMatcher.start(1), strMatcher.end(1)));

        positions.sort(Comparator.comparingInt(p -> p.start));

        // 切割模板
        int lastIndex = 0;
        for (MallIdPosition pos : positions) {
            String part = templateString.substring(lastIndex, pos.start);
            templateParts.add(part.getBytes(StandardCharsets.UTF_8));
            lastIndex = pos.end;
        }
        mallIdCount = positions.size();
        templateParts.add(templateString.substring(lastIndex).getBytes(StandardCharsets.UTF_8));

        // 计算预估长度
        for (byte[] part : templateParts) {
            estimatedMsgLength += part.length;
        }
        estimatedMsgLength += positions.size() * 14; // 12 digits + potential quotes

        LOGGER.info("模板预处理完成，分为 {} 个片段", templateParts.size());
    }

    private static class MallIdPosition {
        final int start, end;

        MallIdPosition(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("KafkaStressTestTool", options);
    }
}

