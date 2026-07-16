package com.lingyun.business.common.util;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 通用数据日志透传处理器
 * 用于打印源数据日志，并将原始内容继续向下游传递
 */
public class DataLogMapFunction extends RichMapFunction<String, String> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DataLogMapFunction.class);

    private final String dataLabel;

    public DataLogMapFunction(String dataLabel) {
        this.dataLabel = dataLabel;
    }

    @Override
    public String map(String value) {
        LOGGER.debug("[数据][{}][{}]", dataLabel, value == null ? "" : value);
        return value;
    }
}
