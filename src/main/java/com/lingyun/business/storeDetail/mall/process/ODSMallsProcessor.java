package com.lingyun.business.storeDetail.mall.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lingyun.business.common.config.ConfigBasedFieldSetter;
import com.lingyun.business.common.config.FieldMappingLoader;
import com.lingyun.business.common.util.DataCleanUtil;
import com.lingyun.business.common.util.JsonUtil;
import com.lingyun.business.common.model.mall.ODSMallsRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * ODS层数据处理器
 * 使用 RichMapFunction 管理生命周期，避免类加载器泄漏
 */
public class ODSMallsProcessor extends RichMapFunction<String, ODSMallsRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ODSMallsProcessor.class);
    private static final String CONFIG_FILE = "mappingFile/storeDetail/mall/ods_malls_field_mapping.json";

    /** 使用 transient 标记，避免序列化；在 open() 中初始化 */
    private transient ObjectMapper objectMapper;
    /** 缓存反射获取的 _children 字段 */
    private transient Field childrenField;
    private transient Map<String, String> fieldMappings;
    private transient Map<String, Method> setterCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 初始化 ObjectMapper
        this.objectMapper = new ObjectMapper();
        this.fieldMappings = FieldMappingLoader.getFieldMappings(CONFIG_FILE);
        this.setterCache = ConfigBasedFieldSetter.initSetterCache(ODSMallsRecord.class);
        // 初始化反射字段
        try {
            this.childrenField = ObjectNode.class.getDeclaredField("_children");
            this.childrenField.setAccessible(true);
            LOGGER.info("ODSMallsProcessor 初始化完成");
        } catch (NoSuchFieldException e) {
            LOGGER.error("无法找到 _children 字段，可能 Jackson 版本不兼容", e);
            this.childrenField = null;
        }
    }

    @Override
    public void close() throws Exception {
        // 清理引用，帮助 GC 回收
        this.objectMapper = null;
        this.childrenField = null;
        this.fieldMappings = null;
        this.setterCache = null;
        LOGGER.info("ODSMallsProcessor 资源已释放");
        super.close();
    }

    @Override
    public ODSMallsRecord map(String jsonStr) throws Exception {
        ODSMallsRecord record = new ODSMallsRecord();
        JsonNode rootNode = JsonUtil.parseJson(jsonStr);

        JsonNode node = extractActualDataNode(rootNode);
        if (node == null) {
            node = rootNode;
        }

        // 使用配置文件模式设置字段
        ConfigBasedFieldSetter<ODSMallsRecord> setter =
                new ConfigBasedFieldSetter<>(node, ODSMallsRecord.class, fieldMappings, setterCache);
        setter.setFields(record);
        cleanDorisTypedFields(record);

        return record;
    }

    private void cleanDorisTypedFields(ODSMallsRecord record) {
        record.setGoodsNum(DataCleanUtil.parseSalesNumWithUnitFallback(
                record.getGoodsNum(), record.getGoodsNumUnit()));
        record.setGoodsSalesNum(DataCleanUtil.parseSalesNumWithUnitFallback(
                record.getGoodsSalesNum(), record.getGoodsSalesNumUnit()));
        record.setFollowerNum(DataCleanUtil.parseSalesNumWithUnitFallback(
                record.getFollowerNum(), record.getFollowerNumUnit()));
        record.setReviewNum(DataCleanUtil.parseSalesNum(record.getReviewNum()));
        record.setMallStar(DataCleanUtil.parseDecimal(record.getMallStar()));
        record.setServerTime(DataCleanUtil.parseSalesNum(record.getServerTime()));
    }

    /**
     * 从嵌套的 ObjectNode._children 结构中提取真正的数据节点
     */
    private JsonNode extractActualDataNode(JsonNode rootNode) {
        // 检查反射字段是否初始化成功
        if (childrenField == null) {
            LOGGER.warn("_children 字段未初始化，无法提取数据节点");
            return null;
        }

        if (!(rootNode instanceof ObjectNode)) {
            LOGGER.debug("根节点为空或不是 ObjectNode，跳过提取");
            return null;
        }

        try {
            ObjectNode current = (ObjectNode) rootNode;

            // 使用实例变量的反射字段访问 _children
            @SuppressWarnings("unchecked")
            Map<String, JsonNode> children = (Map<String, JsonNode>) childrenField.get(current);

            if (children == null || children.isEmpty()) {
                LOGGER.debug("根节点的 _children 为空，跳过提取");
                return null;
            }

            // 获取第一个 entry
            Map.Entry<String, JsonNode> firstEntry = children.entrySet().iterator().next();
            JsonNode firstChild = firstEntry.getValue();

            if (!(firstChild instanceof ObjectNode)) {
                LOGGER.debug("第一个子节点为空或不是 ObjectNode，跳过提取");
                return null;
            }

            ObjectNode firstChildNode = (ObjectNode) firstChild;
            @SuppressWarnings("unchecked")
            Map<String, JsonNode> firstChildChildren = (Map<String, JsonNode>) childrenField.get(firstChildNode);

            if (firstChildChildren == null || firstChildChildren.isEmpty()) {
                return null;
            }

            // 将第一个子节点的 _children 转换为 ObjectNode
            ObjectNode resultNode = objectMapper.createObjectNode();
            for (Map.Entry<String, JsonNode> entry : firstChildChildren.entrySet()) {
                resultNode.set(entry.getKey(), entry.getValue());
            }

            LOGGER.debug("成功提取数据节点，包含 {} 个字段", resultNode.size());
            return resultNode;
        } catch (IllegalAccessException e) {
            LOGGER.error("无法访问 _children 字段", e);
            return null;
        } catch (Exception e) {
            LOGGER.error("提取数据节点失败，使用原始节点", e);
            return null;
        }
    }
}
