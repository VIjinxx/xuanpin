package com.lingyun.business.goodsDetail.category.process;

import com.lingyun.business.common.model.category.DWDCategoryRecord;
import com.lingyun.business.common.model.category.ODSCategoryRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * DWD层类目数据处理器
 * 将ODS层记录转换为DWD层记录,进行字段清洗和转换
 *
 * @author wxx
 */
public class DWDCategoryProcessor extends RichMapFunction<ODSCategoryRecord, DWDCategoryRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DWDCategoryProcessor.class);

    /** 使用 transient 标记,避免序列化 */
    private transient Field[] dwdFields;
    private transient Map<String, Method> odsGetterCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化并缓存 DWD 字段
        Field[] fields = DWDCategoryRecord.class.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
        }
        this.dwdFields = fields;

        // 初始化并缓存 ODS 的所有 getter 方法
        this.odsGetterCache = new HashMap<>();
        for (Method method : ODSCategoryRecord.class.getMethods()) {
            String name = method.getName();
            if (name.startsWith("get") && name.length() > 3 && method.getParameterCount() == 0) {
                String fieldName = Character.toLowerCase(name.charAt(3)) + name.substring(4);
                odsGetterCache.put(fieldName, method);
            }
        }

        LOGGER.info("DWDCategoryProcessor 初始化完成: 缓存 {} 个DWD字段, {} 个ODS getter",
                dwdFields.length, odsGetterCache.size());
    }

    @Override
    public void close() throws Exception {
        this.dwdFields = null;
        this.odsGetterCache = null;
        LOGGER.info("DWDCategoryProcessor 资源已释放");
        super.close();
    }

    @Override
    public DWDCategoryRecord map(ODSCategoryRecord odsRecord) throws Exception {
        if (odsRecord == null) {
            return null;
        }

        DWDCategoryRecord dwdRecord = new DWDCategoryRecord();

        for (Field dwdField : dwdFields) {
            String fieldName = dwdField.getName();
            try {
                String value = extractFieldValue(odsRecord, fieldName);
                if (value != null) {
                    dwdField.set(dwdRecord, value);
                }
            } catch (Exception e) {
                LOGGER.debug("字段: {} 转换失败: {}", fieldName, e.getMessage());
            }
        }

        return dwdRecord;
    }

    /**
     * 从ODS记录中提取字段值
     */
    private String extractFieldValue(ODSCategoryRecord odsRecord, String fieldName) {
        // DWD表的字段都是直接映射,不需要特殊处理
        return invokeGetterAsString(odsRecord, fieldName);
    }

    /**
     * 调用 getter 方法获取字符串值
     */
    private String invokeGetterAsString(ODSCategoryRecord source, String field) {
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
}
