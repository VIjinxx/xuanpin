package com.lingyun.business.common.dao;

import com.lingyun.business.common.util.KafkaDeserializer;
import com.lingyun.business.common.util.PropertiesUtil;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink Kafka 数据访问层
 * 负责管理 Kafka 连接配置和创建 Kafka 数据源
 * 支持为不同任务指定不同的 topic；如果未指定则使用默认 topic 配置
 *
 * @author wxx
 */
public class FlinkKafkaDao {

    private static final Logger logger = LoggerFactory.getLogger(FlinkKafkaDao.class);

    private final String bootstrapServers;
    private final String groupId;
    private final String topic;
    private final String offsetReset;

    /**
     * 构造函数（使用默认配置）
     * 任务可以直接传入 Kafka topic 名称；如果未指定则使用默认 topic 配置
     *
     * @param topicName 可选的 Kafka topic 名称，如果为 null 或空则使用默认 "topic"
     */
    public FlinkKafkaDao(String topicName) {
        this(topicName, null);
    }

    /**
     * 构造函数（使用指定topic和groupId配置）
     * 任务可以直接传入 Kafka topic 名称和 consumer group；如果未指定则使用默认配置
     *
     * @param topicName 可选的 Kafka topic 名称，如果为 null 或空则使用默认 "topic"
     * @param customGroupId 可选的 Kafka consumer groupId，如果为 null 或空则使用默认 "groupId"
     */
    public FlinkKafkaDao(String topicName, String customGroupId) {
        this.bootstrapServers = PropertiesUtil.getProperty("bootstrapServers");
        if (customGroupId != null && !customGroupId.isEmpty()) {
            this.groupId = customGroupId;
        } else {
            this.groupId = PropertiesUtil.getProperty("groupId");
        }
        if (topicName != null && !topicName.isEmpty()) {
            this.topic = topicName;
        } else {
            this.topic = PropertiesUtil.getProperty("topic");
        }
        // 从配置文件读取消费位置，默认为 earliest
        this.offsetReset = PropertiesUtil.getProperty("kafka.offset.reset", "earliest");
    }

    /**
     * 构造函数（使用默认topic配置）
     */
    public FlinkKafkaDao() {
        this(null);
    }

    /**
     * 创建 Kafka 数据源
     *
     * @return KafkaSource 实例
     */
    public KafkaSource<String> createSource() {
        logger.info("创建Kafka数据源: topic={}, servers={}, groupId={}, offsetReset={}",
                topic, bootstrapServers, groupId, offsetReset);

        // 根据配置决定回退策略（当没有已提交offset时使用）
        OffsetResetStrategy resetStrategy = "latest".equalsIgnoreCase(offsetReset)
                ? OffsetResetStrategy.LATEST
                : OffsetResetStrategy.EARLIEST;

        // 优先使用已提交的offset，避免重复消费；没有已提交offset时才使用回退策略
        OffsetsInitializer offsetsInitializer = OffsetsInitializer.committedOffsets(resetStrategy);

        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(offsetsInitializer)
                // 使用容错反序列化器，避免单条消息异常导致整个任务失败
                .setValueOnlyDeserializer(new KafkaDeserializer())
                .build();
    }
}

