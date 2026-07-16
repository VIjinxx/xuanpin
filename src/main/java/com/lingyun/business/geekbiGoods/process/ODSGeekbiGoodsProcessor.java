package com.lingyun.business.geekbiGoods.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.lingyun.business.common.util.JsonUtil;
import com.lingyun.business.geekbiGoods.model.ODSGeekbiGoodsRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;

/**
 * 将 geekbiGoods Kafka 消息转换为一条 ODS 原始记录。
 */
public class ODSGeekbiGoodsProcessor extends RichMapFunction<String, ODSGeekbiGoodsRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ODSGeekbiGoodsProcessor.class);

    @Override
    public ODSGeekbiGoodsRecord map(String jsonStr) {
        if (jsonStr == null || jsonStr.trim().isEmpty()) {
            return null;
        }

        JsonNode root = JsonUtil.parseJson(jsonStr);
        JsonNode data = root == null ? null : root.path("data");
        JsonNode goods = data == null ? null : data.path("goods");
        JsonNode site = data == null ? null : data.path("site");
        Long goodsId = longValue(goods, "goodsId");
        Integer siteId = intValue(site, "id");
        if (data == null || !data.isObject() || goodsId == null || siteId == null) {
            LOGGER.warn("外部数据源商品消息缺少 data.goods.goodsId 或 data.site.id,已跳过");
            return null;
        }

        ODSGeekbiGoodsRecord record = new ODSGeekbiGoodsRecord();
        record.setGoodsId(goodsId);
        record.setSiteId(siteId);
        record.setDate(LocalDate.now().toString());
        record.setRankings(copyNullable(data.get("rankings")));
        record.setSite(copyNullable(data.get("site")));
        record.setStats(copyNullable(data.get("stats")));
        record.setGoods(copyNullable(data.get("goods")));
        record.setHistory(copyNullable(data.get("history")));

        LOGGER.info("外部数据源商品 ODS 解析完成: goodsId={}, siteId={}, historyCount={}",
                goodsId, siteId, record.getHistory() != null && record.getHistory().isArray()
                        ? record.getHistory().size() : 0);
        return record;
    }

    private static JsonNode copyNullable(JsonNode node) {
        return node == null || node.isMissingNode() ? null : node.deepCopy();
    }

    private static Long longValue(JsonNode node, String fieldName) {
        if (node == null || node.isMissingNode() || !node.hasNonNull(fieldName)) {
            return null;
        }
        JsonNode value = node.get(fieldName);
        if (value.canConvertToLong()) {
            return value.longValue();
        }
        try {
            return Long.valueOf(value.asText().trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static Integer intValue(JsonNode node, String fieldName) {
        Long value = longValue(node, fieldName);
        return value == null || value > Integer.MAX_VALUE || value < Integer.MIN_VALUE
                ? null : value.intValue();
    }
}
