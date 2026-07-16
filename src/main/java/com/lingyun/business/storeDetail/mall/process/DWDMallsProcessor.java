package com.lingyun.business.storeDetail.mall.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.lingyun.business.common.model.goods.GoodsSchemaConverter;
import com.lingyun.business.common.model.mall.DWDMallsRecord;
import com.lingyun.business.common.model.mall.ODSMallsRecord;
import com.lingyun.business.common.util.DataCleanUtil;
import com.lingyun.business.common.util.JsonUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DWD层数据处理器
 * 使用 RichMapFunction 管理生命周期，避免类加载器泄漏
 */
public class DWDMallsProcessor extends RichMapFunction<ODSMallsRecord, DWDMallsRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DWDMallsProcessor.class);

    /** 使用 transient 标记，避免序列化；在 open() 中初始化 */
    private transient Field[] dwdFields;
    private transient Map<String, Method> odsGetterCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 初始化并缓存 DWD 字段
        Field[] fields = DWDMallsRecord.class.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
        }
        this.dwdFields = fields;

        // 初始化并缓存 ODS 的所有 getter 方法
        this.odsGetterCache = new HashMap<>();
        for (Method method : ODSMallsRecord.class.getMethods()) {
            String name = method.getName();
            if (name.startsWith("get") && name.length() > 3 && method.getParameterCount() == 0) {
                // getFieldName -> fieldName
                String fieldName = Character.toLowerCase(name.charAt(3)) + name.substring(4);
                odsGetterCache.put(fieldName, method);
            }
        }
        LOGGER.info("DWDMallsProcessor 初始化完成: 缓存 {} 个DWD字段, {} 个ODS getter",
                dwdFields.length, odsGetterCache.size());
    }

    @Override
    public void close() throws Exception {
        // 清理引用，帮助 GC 回收
        this.dwdFields = null;
        this.odsGetterCache = null;
        LOGGER.info("DWDMallsProcessor 资源已释放");
        super.close();
    }

    @Override
    public DWDMallsRecord map(ODSMallsRecord odsRecord) throws Exception {
        DWDMallsRecord dwdRecord = new DWDMallsRecord();

        // 使用缓存的字段数组，避免每次反射获取
        for (Field dwdField : dwdFields) {
            String fieldName = dwdField.getName();
            try {
                Object value;
                switch (fieldName) {
                    case "goodsNum":
                        value = DataCleanUtil.parseSalesNumWithUnitFallback(
                                odsRecord.getGoodsNum(), odsRecord.getGoodsNumUnit());
                        break;
                    // 特殊销量转换
                    case "goodsSalesNum":
                        value = DataCleanUtil.parseSalesNumWithUnitFallback(
                                odsRecord.getGoodsSalesNum(), odsRecord.getGoodsSalesNumUnit());
                        break;
                    case "followerNum":
                        value = DataCleanUtil.parseSalesNum(odsRecord.getFollowerNum());
                        break;
                    case "optList":
                        value = extractOptIdJson(odsRecord.getOptList());
                        break;
                    default:
                        value = extractFieldValue(odsRecord, fieldName);
                        if (value == null) {
                            continue;
                        }
                        break;
                }
                Object converted = GoodsSchemaConverter.convertToParameterType(value, dwdField.getType());
                if (converted != null) {
                    dwdField.set(dwdRecord, converted);
                }
            } catch (Exception e) {
                LOGGER.debug("字段: {} 转换失败: {}", fieldName, e.getMessage());
            }
        }
        return dwdRecord;
    }

    private String extractFieldValue(ODSMallsRecord odsRecord, String fieldName) {
        if (!fieldName.contains("_")) {
            return invokeGetterAsString(odsRecord, fieldName);
        }

        String[] parts = fieldName.split("_");
        String jsonValue = invokeGetterAsString(odsRecord, parts[0]);
        if (jsonValue == null || jsonValue.isEmpty() || "null".equalsIgnoreCase(jsonValue)) {
            return null;
        }
        try {
            JsonNode rootNode = JsonUtil.parseJson(jsonValue);
            if (rootNode == null) {
                return null;
            }
            List<JsonNode> leaves = JsonUtil.collectLeafNodes(rootNode, parts, 1);
            return JsonUtil.leavesToString(leaves);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 使用缓存的 getter 方法调用，避免每次反射查找
     */
    private String invokeGetterAsString(ODSMallsRecord source, String field) {
        Method getter = odsGetterCache.get(field);
        if (getter == null) {
            return null;
        }
        try {
            Object value = getter.invoke(source);
            return value == null ? null : value.toString();
        } catch (Exception e) {
            return null;
        }
    }

    private String extractOptIdJson(String optList) {
        JsonNode rootNode = JsonUtil.parseJson(optList);
        if (rootNode == null) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean isFirst = true;

        if (rootNode.isArray()) {
            for (JsonNode node : rootNode) {
                if (node.has("optId")) {
                    if (!isFirst) {
                        sb.append(",");
                    }
                    sb.append(node);
                    isFirst = false;
                }
            }
        }
        sb.append("]");

        return sb.toString();
    }
}
