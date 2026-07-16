package com.lingyun.business.geekbiGoods.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.lingyun.business.common.util.JsonUtil;
import com.lingyun.business.geekbiGoods.model.DWDGeekbiGoodsRecord;
import com.lingyun.business.geekbiGoods.model.ODSGeekbiGoodsRecord;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 将 ODS 中的 history 数组展开为外部数据源商品日明细。
 */
public class DWDGeekbiGoodsProcessor
        extends RichFlatMapFunction<ODSGeekbiGoodsRecord, DWDGeekbiGoodsRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DWDGeekbiGoodsProcessor.class);
    private static final DateTimeFormatter DORIS_DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void flatMap(
            ODSGeekbiGoodsRecord odsRecord,
            Collector<DWDGeekbiGoodsRecord> out) {
        if (odsRecord == null || odsRecord.getHistory() == null || !odsRecord.getHistory().isArray()) {
            return;
        }

        int emitted = 0;
        for (JsonNode historyItem : odsRecord.getHistory()) {
            DWDGeekbiGoodsRecord record = buildRecord(odsRecord, historyItem);
            if (record == null) {
                continue;
            }
            out.collect(record);
            emitted++;
        }

        LOGGER.info("外部数据源商品 DWD 展开完成: goodsId={}, siteId={}, emitted={}",
                odsRecord.getGoodsId(), odsRecord.getSiteId(), emitted);
    }

    private DWDGeekbiGoodsRecord buildRecord(ODSGeekbiGoodsRecord ods, JsonNode history) {
        String date = dateValue(history == null ? null : history.get("createTime"));
        if (history == null || !history.isObject() || date == null) {
            LOGGER.warn("外部数据源商品 history 缺少有效 createTime,已跳过: goodsId={}, siteId={}",
                    ods.getGoodsId(), ods.getSiteId());
            return null;
        }

        JsonNode goods = ods.getGoods();
        JsonNode site = ods.getSite();
        DWDGeekbiGoodsRecord record = new DWDGeekbiGoodsRecord();
        record.setGoodsId(firstLong(history, "goodsId", ods.getGoodsId()));
        record.setSiteId(ods.getSiteId());
        record.setDate(date);

        record.setRegionId(firstLong(history, "regionId", firstLong(goods, "regionId",
                firstLong(site, "regionId", null))));
        record.setSourceRecordId(textValue(goods, "id"));
        record.setMallId(textValue(goods, "mallId"));
        record.setGoodsName(textValue(goods, "goodsName"));
        record.setGoodsNameCn(textValue(goods, "goodsNameCn"));
        record.setGoodsNameEn(textValue(goods, "goodsNameEn"));
        record.setThumbnail(textValue(goods, "thumbnail"));
        record.setThumbnailCn(textValue(goods, "thumbnailCn"));
        record.setCatIds(copyNullable(goods == null ? null : goods.get("catIds")));
        record.setOptId(longValue(goods, "optId"));
        record.setCatId(leafCategoryId(goods));
        record.setCatItems(copyNullable(goods == null ? null : goods.get("catItems")));
        record.setCurrency(firstText(site, "currency", textValue(goods, "currency")));
        record.setCents(intValue(site, "cents"));
        record.setExchangeRate(decimalValue(site, "exchangeRate", 6));
        record.setSkuInfo(parseJsonField(goods, "sku"));
        record.setStatus(intValue(goods, "status"));
        record.setHostingMode(intValue(goods, "hostingMode"));
        record.setOnSaleTime(datetimeValue(goods, "onSaleTime"));
        record.setGoodsCreateTime(datetimeValue(goods, "createTime"));
        record.setGoodsUpdateTime(datetimeValue(goods, "updateTime"));

        record.setHistoryId(textValue(history, "id"));
        record.setSold(longValue(history, "sold"));
        record.setSales(decimalValue(history, "sales", 4));
        record.setMinPrice(decimalValue(history, "minPrice", 4));
        record.setMaxPrice(decimalValue(history, "maxPrice", 4));
        record.setQuantity(longValue(history, "quantity"));
        record.setGoodsScore(decimalValue(history, "goodsScore", 2));
        record.setReviewNum(longValue(history, "reviewNum"));
        record.setDaySold(longValue(history, "daySold"));
        record.setWeekSold(longValue(history, "weekSold"));
        record.setMonthSold(longValue(history, "monthSold"));
        record.setDaySales(decimalValue(history, "daySales", 4));
        record.setWeekSales(decimalValue(history, "weekSales", 4));
        record.setMonthSales(decimalValue(history, "monthSales", 4));
        record.setDaySoldRate(decimalValue(history, "daySoldRate", 6));
        record.setWeekSoldRate(decimalValue(history, "weekSoldRate", 6));
        record.setMonthSoldRate(decimalValue(history, "monthSoldRate", 6));
        record.setDaySalesRate(decimalValue(history, "daySalesRate", 6));
        record.setWeekSalesRate(decimalValue(history, "weekSalesRate", 6));
        record.setMonthSalesRate(decimalValue(history, "monthSalesRate", 6));
        record.setDayClickNum(longValue(history, "dayClickNum"));
        record.setWeekClickNum(longValue(history, "weekClickNum"));
        record.setMonthClickNum(longValue(history, "monthClickNum"));
        record.setDayExposureNum(longValue(history, "dayExposureNum"));
        record.setWeekExposureNum(longValue(history, "weekExposureNum"));
        record.setMonthExposureNum(longValue(history, "monthExposureNum"));
        record.setDayClickRate(decimalValue(history, "dayClickRate", 6));
        record.setWeekClickRate(decimalValue(history, "weekClickRate", 6));
        record.setMonthClickRate(decimalValue(history, "monthClickRate", 6));
        record.setDayClickGrowthRate(decimalValue(history, "dayClickGrowthRate", 6));
        record.setWeekClickGrowthRate(decimalValue(history, "weekClickGrowthRate", 6));
        record.setMonthClickGrowthRate(decimalValue(history, "monthClickGrowthRate", 6));
        record.setDayExposureGrowthRate(decimalValue(history, "dayExposureGrowthRate", 6));
        record.setWeekExposureGrowthRate(decimalValue(history, "weekExposureGrowthRate", 6));
        record.setMonthExposureGrowthRate(decimalValue(history, "monthExposureGrowthRate", 6));
        record.setHistoryCreateTime(datetimeValue(history, "createTime"));
        record.setHistoryUpdateTime(datetimeValue(history, "updateTime"));
        return record;
    }

    private static JsonNode parseJsonField(JsonNode node, String fieldName) {
        if (node == null || !node.hasNonNull(fieldName)) {
            return null;
        }
        JsonNode value = node.get(fieldName);
        if (value.isArray() || value.isObject()) {
            return value.deepCopy();
        }
        if (!value.isTextual() || value.asText().trim().isEmpty()) {
            return null;
        }
        JsonNode parsed = JsonUtil.parseJson(value.asText());
        return parsed != null && (parsed.isArray() || parsed.isObject()) ? parsed : null;
    }

    private static Long leafCategoryId(JsonNode goods) {
        JsonNode catItems = goods == null ? null : goods.get("catItems");
        if (catItems == null || !catItems.isArray()) {
            return null;
        }
        for (JsonNode item : catItems) {
            if (item.path("isLeaf").asBoolean(false)) {
                return longValue(item, "catId");
            }
        }
        return null;
    }

    private static String dateValue(JsonNode value) {
        String datetime = datetimeValue(value);
        if (datetime == null || datetime.length() < 10) {
            return null;
        }
        try {
            return LocalDate.parse(datetime.substring(0, 10)).toString();
        } catch (Exception e) {
            return null;
        }
    }

    private static String datetimeValue(JsonNode node, String fieldName) {
        return node == null ? null : datetimeValue(node.get(fieldName));
    }

    private static String datetimeValue(JsonNode value) {
        if (value == null || value.isNull()) {
            return null;
        }
        String raw = value.asText().trim();
        if (raw.isEmpty()) {
            return null;
        }
        try {
            return OffsetDateTime.parse(raw).toLocalDateTime().format(DORIS_DATETIME_FORMATTER);
        } catch (Exception ignored) {
            try {
                return LocalDateTime.parse(raw).format(DORIS_DATETIME_FORMATTER);
            } catch (Exception secondIgnored) {
                return raw.length() >= 19
                        ? raw.substring(0, 19).replace('T', ' ')
                        : null;
            }
        }
    }

    private static JsonNode copyNullable(JsonNode node) {
        return node == null || node.isNull() || node.isMissingNode() ? null : node.deepCopy();
    }

    private static String firstText(JsonNode node, String fieldName, String fallback) {
        String value = textValue(node, fieldName);
        return value == null ? fallback : value;
    }

    private static String textValue(JsonNode node, String fieldName) {
        if (node == null || !node.hasNonNull(fieldName)) {
            return null;
        }
        String value = node.get(fieldName).asText();
        return value == null || value.trim().isEmpty() ? null : value;
    }

    private static Long firstLong(JsonNode node, String fieldName, Long fallback) {
        Long value = longValue(node, fieldName);
        return value == null ? fallback : value;
    }

    private static Long longValue(JsonNode node, String fieldName) {
        if (node == null || !node.hasNonNull(fieldName)) {
            return null;
        }
        JsonNode value = node.get(fieldName);
        if (value.canConvertToLong()) {
            return value.longValue();
        }
        try {
            return new BigDecimal(value.asText().trim()).longValueExact();
        } catch (Exception e) {
            return null;
        }
    }

    private static Integer intValue(JsonNode node, String fieldName) {
        Long value = longValue(node, fieldName);
        return value == null || value > Integer.MAX_VALUE || value < Integer.MIN_VALUE
                ? null : value.intValue();
    }

    private static BigDecimal decimalValue(JsonNode node, String fieldName, int scale) {
        if (node == null || !node.hasNonNull(fieldName)) {
            return null;
        }
        try {
            return new BigDecimal(node.get(fieldName).asText()).setScale(scale, RoundingMode.HALF_UP);
        } catch (Exception e) {
            return null;
        }
    }
}
