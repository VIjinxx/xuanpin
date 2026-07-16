package com.lingyun.business.keywordSearch.html.category.process;

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
 * DWD层数据处理器 - 关键词搜索页类目数据
 * 使用 RichMapFunction 管理生命周期，避免类加载器泄漏
 */
public class DWDKeyCategoryProcessor extends RichMapFunction<ODSCategoryRecord, DWDCategoryRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DWDKeyCategoryProcessor.class);

    private transient Field[] dwdFields;
    private transient Map<String, Method> odsGetterCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        Field[] fields = DWDCategoryRecord.class.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
        }
        this.dwdFields = fields;

        this.odsGetterCache = new HashMap<>();
        for (Method method : ODSCategoryRecord.class.getMethods()) {
            String name = method.getName();
            if (name.startsWith("get") && name.length() > 3 && method.getParameterCount() == 0) {
                String fieldName = Character.toLowerCase(name.charAt(3)) + name.substring(4);
                odsGetterCache.put(fieldName, method);
            }
        }

        LOGGER.info("DWDKeyCategoryProcessor 初始化完成: 缓存 {} 个DWD字段, {} 个ODS getter",
                dwdFields.length, odsGetterCache.size());
    }

    @Override
    public void close() throws Exception {
        this.dwdFields = null;
        this.odsGetterCache = null;
        LOGGER.info("DWDKeyCategoryProcessor 资源已释放");
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

    private String extractFieldValue(ODSCategoryRecord source, String fieldName) {
        Method getter = odsGetterCache.get(fieldName);
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
