package com.lingyun.business.common.model.goods;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.LocalDate;
import lombok.Data;

/**
 * 通用 DWD 层商品记录，对应基础商品表设计。
 */
@Data
public class DWDGoodsRecord {
    /** 商品id */
    private Long goodsId;

    /** 商品所在站点，全托管商品不区分站点，该字段直接为空，半托管商品区分站点，该字段不为空 */
    private Long siteId;

    /** 语言 */
    private String lang;

    /** 选择的货币单位 */
    private String selectedCurrency;

    /** 商品信息统计日期 */
    private LocalDate date;

    /** 商品所属店铺id */
    private String mallId;

    /** 前端类目id */
    private Long optId;

    /** 商家后台类目id */
    private Long catId;

    /** 商品标题 */
    private String title;

    /** 商品链接 */
    private String linkUrl;

    /** 销售数量 */
    private Long salesNum;

    /** 是否在售 */
    private Long isOnsale;

    /** 是否展示原价 */
    private Boolean isShowMarketPrice;

    /** 评论数量 */
    private Long reviewNum;

    /** 商品评分 */
    private Double goodsScore;

    /** 销售价格最低值(单位分) */
    private Long minOnSalePrice;

    /** 销售价格最高值(单位分) */
    private Long maxOnSalePrice;

    /** 价格信息 */
    private JsonNode priceInfo;

    /** 图片信息 */
    private JsonNode image;

    /** 品牌卡片 */
    private JsonNode brandCard;

    /** 指导书文件 */
    private JsonNode guideFile;

    /** 商品图片链接 */
    private String imageUrl;

    /** 略缩图链接 */
    private String thumbUrl;

    /** 商品视频信息 */
    private JsonNode video;

    /** 商品是否可见 */
    private Boolean visible;

    /** 商品页面描述信息 */
    private String pageAlt;

    /** 是否半托管 */
    private Boolean semiManaged;

    /** 销量占库存百分比(存疑) */
    private Long soldQuantityPercent;

    /** 仓库类型 */
    private Long wareHouseType;

    /** 是否为本地商品 */
    private Long isLocalGoods;

    /** 是否为成人用品 */
    private Boolean adultGoods;

    /** 商品轮播图列表 */
    private JsonNode gallery;

    /** 商品属性 */
    private JsonNode goodsProperty;

    /** 尺码指南 */
    private JsonNode sizeGuide;

    /** 商品的skc信息列表 */
    private JsonNode skcList;

    /** 规格id列表 */
    private String specIds;

    /** sku信息列表 */
    private JsonNode skuList;

    /** 关键词 */
    private String trackingKey;

    /** 类型 */
    private Long type;

    /** 是否支持促销 */
    private Boolean supportPromotion;

    /** 尺码表信息 */
    private JsonNode sizeSpecModule;

    /** 规格相关的信息 */
    private JsonNode specCustom;

    /** 商品详情图列表(展开) */
    private JsonNode productDetailFlatList;

    /** 商品详情图信息 */
    private JsonNode productDetail;

    /** 展示结束时间 */
    private Long displayEndTime;

    /** 类目信息列表 */
    private JsonNode crumbOptList;

}
