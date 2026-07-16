package com.lingyun.business.geekbiGoods.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

/**
 * 外部数据源商品 ODS 记录。
 */
@Data
public class ODSGeekbiGoodsRecord {
    private Long goodsId;
    private Integer siteId;
    private String date;
    private JsonNode rankings;
    private JsonNode site;
    private JsonNode stats;
    private JsonNode goods;
    private JsonNode history;
}
