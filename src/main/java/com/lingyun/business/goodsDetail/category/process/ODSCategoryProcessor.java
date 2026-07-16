package com.lingyun.business.goodsDetail.category.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.lingyun.business.common.config.ConfigBasedFieldSetter;
import com.lingyun.business.common.config.FieldMappingLoader;
import com.lingyun.business.common.util.JsonUtil;
import com.lingyun.business.common.model.category.ODSCategoryRecord;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ODS层类目数据处理器
 * 使用 RichFlatMapFunction 将一条商品详情页Kafka数据按 crumbOptList 展开为多条ODS记录
 *
 * @author wxx
 */
public class ODSCategoryProcessor extends RichFlatMapFunction<String, ODSCategoryRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ODSCategoryProcessor.class);
    private static final Pattern FLATTENED_CRUMB_OPT_LIST_INDEX =
            Pattern.compile("^crumbOptList(?:\\[(\\d+)\\]|\\.(\\d+))(?:\\.|$)");

    /** 商品详情页类目映射配置文件 */
    private static final String MAPPING_FILE =
            "mappingFile/goodsDetail/ods_goods_detail_front_category_mapping.json";

    private transient Map<String, String> fieldMappings;
    private transient Map<String, Method> setterCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.fieldMappings = FieldMappingLoader.getFieldMappings(MAPPING_FILE);
        this.setterCache = ConfigBasedFieldSetter.initSetterCache(ODSCategoryRecord.class);
        LOGGER.info("ODSCategoryProcessor 初始化完成,映射配置: {}", MAPPING_FILE);
    }

    @Override
    public void close() throws Exception {
        this.fieldMappings = null;
        this.setterCache = null;
        LOGGER.info("ODSCategoryProcessor 资源已释放");
        super.close();
    }

    @Override
    public void flatMap(String jsonStr, Collector<ODSCategoryRecord> out) throws Exception {
        if (jsonStr == null || jsonStr.isEmpty()) {
            return;
        }

        try {
            JsonNode rootNode = JsonUtil.parseJson(jsonStr);
            List<Integer> crumbIndices = findCrumbOptListIndices(rootNode);

            for (Integer crumbIndex : crumbIndices) {
                try {
                    ODSCategoryRecord record = buildRecord(rootNode, crumbIndex);
                    if (record.getOptId() != null && !record.getOptId().isEmpty()) {
                        out.collect(record);
                    }
                } catch (Exception e) {
                    LOGGER.debug("解析商品详情页类目 crumbOptList[{}] 失败: {}", crumbIndex, e.getMessage());
                }
            }
        } catch (Exception e) {
            LOGGER.error("解析商品详情页类目数据失败: {}", e.getMessage(), e);
        }
    }

    private ODSCategoryRecord buildRecord(JsonNode rootNode, int crumbIndex) throws Exception {
        ODSCategoryRecord record = new ODSCategoryRecord();
        Map<String, String> pathVariables = Collections.singletonMap("i", String.valueOf(crumbIndex));
        ConfigBasedFieldSetter<ODSCategoryRecord> setter =
                new ConfigBasedFieldSetter<>(rootNode, ODSCategoryRecord.class,
                        fieldMappings, setterCache, true, pathVariables);
        setter.setFields(record);
        return record;
    }

    private List<Integer> findCrumbOptListIndices(JsonNode rootNode) {
        TreeSet<Integer> indices = new TreeSet<>();
        JsonNode crumbOptList = rootNode.get("crumbOptList");

        if (crumbOptList != null && crumbOptList.isArray()) {
            for (int i = 0; i < crumbOptList.size(); i++) {
                indices.add(i);
            }
            return new ArrayList<>(indices);
        }

        if (crumbOptList != null && crumbOptList.isObject()) {
            Iterator<String> fieldNames = crumbOptList.fieldNames();
            while (fieldNames.hasNext()) {
                Integer index = parseNonNegativeInteger(fieldNames.next());
                if (index != null) {
                    indices.add(index);
                }
            }
            if (!indices.isEmpty()) {
                return new ArrayList<>(indices);
            }
        }

        Iterator<String> fieldNames = rootNode.fieldNames();
        while (fieldNames.hasNext()) {
            Matcher matcher = FLATTENED_CRUMB_OPT_LIST_INDEX.matcher(fieldNames.next());
            if (matcher.find()) {
                String index = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
                indices.add(Integer.parseInt(index));
            }
        }

        return new ArrayList<>(indices);
    }

    private Integer parseNonNegativeInteger(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return null;
            }
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
