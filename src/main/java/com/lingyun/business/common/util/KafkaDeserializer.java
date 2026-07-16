package com.lingyun.business.common.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 容错的字符串反序列化器
 * 当消息反序列化失败时返回null而不是抛出异常
 *
 * @author wxx
 */
public class KafkaDeserializer implements DeserializationSchema<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDeserializer.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String deserialize(byte[] message) {
        if (message == null || message.length == 0) {
            LOGGER.warn("【Kafka反序列化】收到空消息，跳过");
            return null;
        }
        try {
            String result = new String(message, StandardCharsets.UTF_8);
            return result;
        } catch (Exception e) {
            LOGGER.error("【Kafka反序列化失败】消息长度: {}, 错误: {}", message.length, e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }
}

