package com.lingyun.business.common.model.goods;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.LocalDate;
import lombok.Data;

/**
 * 通用 DWS 层商品聚合记录，对应商品聚合表设计。
 */
@Data
public class DWSGoodsRecord {
    /** 商品id */
    private Long goodsId;

    /** 商品所在站点 */
    private Long siteId;

    /** 商品信息统计日期 */
    private LocalDate date;

    /** 商品英文标题 */
    private String goodsTitleEnglish;

    /** 商品中文标题 */
    private String goodsTitleChinese;

    /** 商品所属店铺id */
    private String mallId;

    /** 商品主图 */
    private String mainImageUrl;

    /** 略缩图链接 */
    private String thumbUrl;

    /** 商品轮播图列表 */
    private JsonNode carouselImageUrls;

    /** 商品的temu前端的类目id */
    private Long optId;

    /** 商品的temu后端的类目id */
    private Long catId;

    /** 是否为成人用品 */
    private Boolean isAdultGoods;

    /** 商品视频信息 */
    private JsonNode videoInfo;

    /** 属性列表 */
    private JsonNode propertyList;

    /** 当前销售数量 */
    private Long salesNum;

    /** 商品售卖货币类型 */
    private String currency;

    /** 商品当前售卖规格最低价格 */
    private Long minOnSalePrice;

    /** 商品当前售卖规格最高价格 */
    private Long maxOnSalePrice;

    /** 商品当前评分 */
    private Double score;

    /** 商品当前评论数 */
    private Long reviewNum;

    /** 商品当前库存状态 */
    private Long stockStatus;

    /** 商品当前库存数量 */
    private Long stockQuantity;

    /** 在该站点上架时间 */
    private LocalDate addToRegionTime;

    /** sku信息 */
    private JsonNode skuInfoList;

    /** 该商品数据第一次被添加时间 */
    private LocalDate firstAddTime;

    /** 该商品数据上一次更新时间 */
    private LocalDate lastUpdateTime;

}
