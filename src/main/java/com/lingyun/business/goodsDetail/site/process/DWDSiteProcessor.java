package com.lingyun.business.goodsDetail.site.process;

import com.lingyun.business.common.model.site.DWDSiteRecord;
import com.lingyun.business.common.model.site.ODSSiteRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * DWD层数据处理器
 * 使用 RichMapFunction 管理生命周期，避免类加载器泄漏
 */
public class DWDSiteProcessor extends RichMapFunction<ODSSiteRecord, DWDSiteRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DWDSiteProcessor.class);

    /** 使用 transient 标记，避免序列化；在 open() 中初始化 */
    private transient Field[] dwdFields;
    private transient Map<String, Method> odsGetterCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 初始化并缓存 DWD 字段
        Field[] fields = DWDSiteRecord.class.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
        }
        this.dwdFields = fields;

        // 初始化并缓存 ODS 的所有 getter 方法
        this.odsGetterCache = new HashMap<>();
        for (Method method : ODSSiteRecord.class.getMethods()) {
            String name = method.getName();
            if (name.startsWith("get") && name.length() > 3 && method.getParameterCount() == 0) {
                // getFieldName -> fieldName
                String fieldName = Character.toLowerCase(name.charAt(3)) + name.substring(4);
                odsGetterCache.put(fieldName, method);
            }
        }
        LOGGER.info("DWDSiteProcessor 初始化完成: 缓存 {} 个DWD字段, {} 个ODS getter",
                dwdFields.length, odsGetterCache.size());
    }

    @Override
    public void close() throws Exception {
        // 清理引用，帮助 GC 回收
        this.dwdFields = null;
        this.odsGetterCache = null;
        LOGGER.info("DWDSiteProcessor 资源已释放");
        super.close();
    }

    @Override
    public DWDSiteRecord map(ODSSiteRecord odsRecord) throws Exception {
        DWDSiteRecord dwdRecord = new DWDSiteRecord();

        // 使用缓存的字段数组，避免每次反射获取
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
     * 使用缓存的 getter 方法调用，避免每次反射查找
     */
    private String extractFieldValue(ODSSiteRecord source, String field) {
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


