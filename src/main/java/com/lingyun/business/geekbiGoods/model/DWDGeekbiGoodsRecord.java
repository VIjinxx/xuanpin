package com.lingyun.business.geekbiGoods.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

import java.math.BigDecimal;

/**
 * 外部数据源商品历史日明细记录。
 */
@Data
public class DWDGeekbiGoodsRecord {
    private Long goodsId;
    private Integer siteId;
    private String date;

    private Long regionId;
    private String sourceRecordId;
    private String mallId;
    private String goodsName;
    private String goodsNameCn;
    private String goodsNameEn;
    private String thumbnail;
    private String thumbnailCn;
    private JsonNode catIds;
    private Long optId;
    private Long catId;
    private JsonNode catItems;
    private String currency;
    private Integer cents;
    private BigDecimal exchangeRate;
    private JsonNode skuInfo;
    private Integer status;
    private Integer hostingMode;
    private String onSaleTime;
    private String goodsCreateTime;
    private String goodsUpdateTime;

    private String historyId;
    private Long sold;
    private BigDecimal sales;
    private BigDecimal minPrice;
    private BigDecimal maxPrice;
    private Long quantity;
    private BigDecimal goodsScore;
    private Long reviewNum;
    private Long daySold;
    private Long weekSold;
    private Long monthSold;
    private BigDecimal daySales;
    private BigDecimal weekSales;
    private BigDecimal monthSales;
    private BigDecimal daySoldRate;
    private BigDecimal weekSoldRate;
    private BigDecimal monthSoldRate;
    private BigDecimal daySalesRate;
    private BigDecimal weekSalesRate;
    private BigDecimal monthSalesRate;
    private Long dayClickNum;
    private Long weekClickNum;
    private Long monthClickNum;
    private Long dayExposureNum;
    private Long weekExposureNum;
    private Long monthExposureNum;
    private BigDecimal dayClickRate;
    private BigDecimal weekClickRate;
    private BigDecimal monthClickRate;
    private BigDecimal dayClickGrowthRate;
    private BigDecimal weekClickGrowthRate;
    private BigDecimal monthClickGrowthRate;
    private BigDecimal dayExposureGrowthRate;
    private BigDecimal weekExposureGrowthRate;
    private BigDecimal monthExposureGrowthRate;
    private String historyCreateTime;
    private String historyUpdateTime;
}
