package com.lingyun.business.common.job;

import com.lingyun.business.common.sink.DorisBatchSink;
import com.lingyun.business.common.sink.DorisTableConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

import static com.lingyun.business.common.util.PropertiesUtil.configureCheckpoint;

/**
 * Flink Job 抽象基类
 * 封装 ODS -> DWD 数据处理流程的通用逻辑
 *
 * @param <ODS> ODS层记录类型
 * @param <DWD> DWD层记录类型
 * @author wxx
 */
public abstract class BaseFlinkJob<ODS, DWD> {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 执行 Flink 任务
     *
     * @throws Exception 执行异常
     */
    public void execute() throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureCheckpoint(env);

        // 2. 获取数据源
        DataStream<String> sourceStream = createSourceStream(env);

        // 3. ODS层处理
        SingleOutputStreamOperator<ODS> odsStream = processOdsLayer(sourceStream);

        // 4. ODS写入Doris
        writeOdsToDoris(odsStream);

        // 5. DWD层处理
        SingleOutputStreamOperator<DWD> dwdStream = processDwdLayer(odsStream);

        // 6. DWD写入Doris
        writeDwdToDoris(dwdStream);

        // 7. 启动任务
        logger.info("启动Flink任务: {}", getJobName());
        env.execute(getJobName());
    }

    /**
     * 创建数据源流
     * 子类可以覆盖此方法以使用不同的数据源（Kafka、File等）
     *
     * @param env 执行环境
     * @return 源数据流
     */
    protected abstract DataStream<String> createSourceStream(StreamExecutionEnvironment env);

    /**
     * ODS层处理
     */
    private SingleOutputStreamOperator<ODS> processOdsLayer(DataStream<String> sourceStream) {
        if (getOdsFlatMapProcessor() != null) {
            // 使用 flatMap（一对多）
            return sourceStream
                    .flatMap(getOdsFlatMapProcessor())
                    .filter(getOdsFilter())
                    .name("ODS_Process")
                    .uid("ods-process");
        } else {
            // 使用 map（一对一）
            return sourceStream
                    .map(getOdsMapProcessor())
                    .filter(getOdsFilter())
                    .name("ODS_Process")
                    .uid("ods-process");
        }
    }

    /**
     * ODS写入Doris
     */
    private void writeOdsToDoris(SingleOutputStreamOperator<ODS> odsStream) {
        odsStream
                .map(record -> getOdsToJsonFunction().apply(record))
                .name("ODS_ToJson")
                .uid("ods-to-json")
                .filter((String json) -> json != null && !json.isEmpty())
                .sinkTo(DorisBatchSink.createODSDorisSink(getOdsTableConfig()))
                .name("ODS_Sink")
                .uid("ods-sink");
    }

    /**
     * DWD层处理
     */
    private SingleOutputStreamOperator<DWD> processDwdLayer(SingleOutputStreamOperator<ODS> odsStream) {
        return odsStream
                .map(getDwdProcessor())
                .filter(getDwdFilter())
                .name("DWD_Process")
                .uid("dwd-process");
    }

    /**
     * DWD写入Doris
     */
    private void writeDwdToDoris(SingleOutputStreamOperator<DWD> dwdStream) {
        dwdStream
                .map(record -> getDwdToJsonFunction().apply(record))
                .name("DWD_ToJson")
                .uid("dwd-to-json")
                .filter((String json) -> json != null && !json.isEmpty())
                .sinkTo(DorisBatchSink.createDWDDorisSink(getDwdTableConfig()))
                .name("DWD_Sink")
                .uid("dwd-sink");
    }

    // ========== 子类需要实现的抽象方法 ==========

    /**
     * 获取任务名称
     */
    protected abstract String getJobName();

    /**
     * 获取 ODS FlatMap 处理器（一对多场景）
     * 如果返回 null，则使用 getOdsMapProcessor()
     */
    protected FlatMapFunction<String, ODS> getOdsFlatMapProcessor() {
        return null;
    }

    /**
     * 获取 ODS Map 处理器（一对一场景）
     * 仅当 getOdsFlatMapProcessor() 返回 null 时使用
     */
    protected MapFunction<String, ODS> getOdsMapProcessor() {
        return null;
    }

    /**
     * 获取 ODS 过滤器
     */
    protected abstract FilterFunction<ODS> getOdsFilter();

    /**
     * 获取 ODS 转 JSON 函数
     */
    protected abstract Function<ODS, String> getOdsToJsonFunction();

    /**
     * 获取 ODS 表配置
     */
    protected abstract DorisTableConfig getOdsTableConfig();

    /**
     * 获取 DWD 处理器
     */
    protected abstract MapFunction<ODS, DWD> getDwdProcessor();

    /**
     * 获取 DWD 过滤器
     */
    protected abstract FilterFunction<DWD> getDwdFilter();

    /**
     * 获取 DWD 转 JSON 函数
     */
    protected abstract Function<DWD, String> getDwdToJsonFunction();

    /**
     * 获取 DWD 表配置
     */
    protected abstract DorisTableConfig getDwdTableConfig();
}
