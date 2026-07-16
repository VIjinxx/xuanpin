package com.lingyun.business.goodsDetail.goods.process;

import com.lingyun.business.common.util.DataCleanUtil;
import com.lingyun.business.common.model.goods.DWDGoodsRecord;
import com.lingyun.business.common.model.goods.GoodsSchema;
import com.lingyun.business.common.model.goods.GoodsSchemaConverter;
import com.lingyun.business.common.model.goods.ODSGoodsRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.Introspector;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * DWD层商品数据处理器
 * 使用 RichMapFunction 管理生命周期,避免类加载器泄漏
 */
public class DWDGoodsProcessor extends RichMapFunction<ODSGoodsRecord, DWDGoodsRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DWDGoodsProcessor.class);

    /** 使用 transient 标记,避免序列化;在 open() 中初始化 */
    private transient Map<String, Method> odsGetterCache;
    private transient Map<String, Method> dwdSetterCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 初始化并缓存 ODS 的所有 getter 方法
        this.odsGetterCache = new LinkedHashMap<>();
        for (Method method : ODSGoodsRecord.class.getMethods()) {
            String name = method.getName();
            if (name.startsWith("get") && name.length() > 3 && method.getParameterCount() == 0) {
                String fieldName = Introspector.decapitalize(name.substring(3));
                odsGetterCache.put(fieldName, method);
                odsGetterCache.put(GoodsSchema.normalizeFieldName(fieldName), method);
                odsGetterCache.put(GoodsSchema.normalizeFieldName(fieldName).toLowerCase(), method);
            }
        }

        this.dwdSetterCache = new LinkedHashMap<>();
        for (Method method : DWDGoodsRecord.class.getMethods()) {
            String name = method.getName();
            if (name.startsWith("set") && name.length() > 3 && method.getParameterCount() == 1) {
                String fieldName = Introspector.decapitalize(name.substring(3));
                dwdSetterCache.put(fieldName, method);
                dwdSetterCache.put(GoodsSchema.normalizeFieldName(fieldName), method);
                dwdSetterCache.put(GoodsSchema.normalizeFieldName(fieldName).toLowerCase(), method);
            }
        }

        LOGGER.info("DWDGoodsProcessor 初始化完成: 缓存 {} 个DWD setter, {} 个ODS getter",
                dwdSetterCache.size(), odsGetterCache.size());
    }

    @Override
    public void close() throws Exception {
        // 清理引用,帮助 GC 回收
        this.odsGetterCache = null;
        this.dwdSetterCache = null;
        LOGGER.info("DWDGoodsProcessor 资源已释放");
        super.close();
    }

    @Override
    public DWDGoodsRecord map(ODSGoodsRecord odsRecord) throws Exception {
        DWDGoodsRecord dwdRecord = new DWDGoodsRecord();

        for (GoodsSchema.FieldDef field : GoodsSchema.dwdFields()) {
            String fieldName = field.getName();
            try {
                Object value;
                if ("date".equals(fieldName)) {
                    value = LocalDate.now();
                } else {
                    value = extractFieldValue(odsRecord, fieldName);
                    if ("salesNum".equals(fieldName) && value != null) {
                        value = DataCleanUtil.parseSalesNum(String.valueOf(value));
                    }
                    value = GoodsSchemaConverter.convertObject(value, field.getType());
                }
                if (value != null) {
                    setFieldValue(dwdRecord, fieldName, value);
                }
            } catch (Exception e) {
                LOGGER.debug("字段: {} 转换失败: {}", fieldName, e.getMessage());
            }
        }
        return dwdRecord;
    }

    private Object extractFieldValue(ODSGoodsRecord odsRecord, String fieldName) {
        return invokeGetter(odsRecord, fieldName);
    }

    /**
     * 使用缓存的 getter 方法调用,避免每次反射查找
     */
    private Object invokeGetter(ODSGoodsRecord source, String field) {
        String normalizedField = GoodsSchema.normalizeFieldName(field);
        Method getter = odsGetterCache.get(normalizedField);
        if (getter == null) {
            getter = odsGetterCache.get(normalizedField.toLowerCase());
        }
        if (getter == null) {
            return null;
        }
        try {
            return getter.invoke(source);
        } catch (Exception e) {
            return null;
        }
    }

    private void setFieldValue(DWDGoodsRecord record, String fieldName, Object value) throws Exception {
        String normalizedFieldName = GoodsSchema.normalizeFieldName(fieldName);
        Method setter = dwdSetterCache.get(normalizedFieldName);
        if (setter == null) {
            setter = dwdSetterCache.get(normalizedFieldName.toLowerCase());
        }
        if (setter != null) {
            Object converted = GoodsSchemaConverter.convertToParameterType(value, setter.getParameterTypes()[0]);
            if (converted != null) {
                setter.invoke(record, converted);
            }
        }
    }
}
