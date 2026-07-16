package com.lingyun.business.common.model.goods;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

/**
 * 通用 ODS 层商品记录，对应原始商品表设计。
 */
@Data
public class ODSGoodsRecord {
    /** 商品id */
    private Long goodsId;

    /** 商品所属店铺id */
    private String mallId;

    /** 商家类型编码 */
    private Long sellerType;

    /** 当前skuid */
    private String currentSkuId;

    /** 默认的skuid */
    private String defaultSkuId;

    /** 商品采集站点 */
    private Long siteId;

    /** 商品信息统计日期 */
    private String date;

    /** 选择的语言 */
    private String selectedLang;

    /** 语言 */
    private String lang;

    /** 选择的货币单位 */
    private String selectedCurrency;

    /** 前端类目id */
    private Long optId;

    /** optType */
    private Long optType;

    /** optLevel */
    private Long optLevel;

    /** 商家后台类目id */
    private Long catId;

    /** 商家后台类目的多层级id信息 */
    private JsonNode catInfo;

    /** 商品标题 */
    private String title;

    /** 商品标题 */
    private String goodsName;

    /** 商品链接 */
    private String linkUrl;

    /** 活动类型 */
    private Long activityType;

    /** 销售贴士 */
    private String sideSalesTip;

    /** 销售贴士 */
    private String salesTip;

    /** 销售文本列表 */
    private JsonNode salesTipText;

    /** 销售文本列表 */
    private JsonNode salesTipTextList;

    /** 商家承担优惠金额 */
    private String businessReduction;

    /** 是否在售 */
    private Long isOnsale;

    /** 是否展示原价 */
    private Boolean isShowMarketPrice;

    /** 是否为skc */
    private Boolean isSkc;

    /** qty */
    private Long qty;

    /** status */
    private Long status;

    /** soldOutSubscribeStatus */
    private Long soldOutSubscribeStatus;

    /** 销售数量 */
    private Long salesNum;

    /** 销售数量 */
    private Long soldQuantity;

    /** 评论数量 */
    private Long reviewNum;

    /** 评论数量提示 */
    private String commentNumTips;

    /** 评价列表排序样式标识 */
    private Long sortTypeStyle;

    /** 商品评分 */
    private Double goodsScore;

    /** 商品评分 */
    private Double showScore;

    /** 商品综合评价得分 */
    private String reviewScore;

    /** 是否隐藏评论 */
    private Boolean hiddenComment;

    /** seo链接 */
    private String seoLinkUrl;

    /** 默认选择的规格 */
    private JsonNode defaultSelectSpec;

    /** 销售价格最低值(单位分) */
    private Long minOnSalePrice;

    /** 销售价格最高值(单位分) */
    private Long maxOnSalePrice;

    /** 价格信息 */
    private JsonNode priceInfo;

    /** 销售价格富文本信息 */
    private JsonNode salePriceRich;

    /** minToMaxSalePriceRich */
    private JsonNode minToMaxSalePriceRich;

    /** 图片信息 */
    private JsonNode image;

    /** 品牌卡片 */
    private JsonNode brandCard;

    /** 指导书文件 */
    private JsonNode guideFile;

    /** 缩略图链接 */
    private String hdThumbUrl;

    /** 略缩图链接 */
    private String thumbUrl;

    /** 商品图片链接 */
    private String goodsImage;

    /** 商品图片链接 */
    private String imageUrl;

    /** 商品图片链接 */
    private String longThumbUrl;

    /** 用户原始搜索关键词 */
    private String searchKey;

    /** queryReleScore */
    private Double queryReleScore;

    /** goodRankList */
    private JsonNode goodRankList;

    /** 商品视频信息 */
    private JsonNode video;

    /** 商品是否可见 */
    private Boolean visible;

    /** 商品页面描述信息 */
    private String pageAlt;

    /** itemType */
    private Long itemType;

    /** HWRatio */
    private Long HWRatio;

    /** 是否半托管 */
    private Boolean semiManaged;

    /** 折扣文本信息 */
    private JsonNode benefitText;

    /** 销量占库存百分比(存疑) */
    private Long soldQuantityPercent;

    /** 仓库类型 */
    private Long wareHouseType;

    /** 是否为本地商品 */
    private Long isLocalGoods;

    /** itemId */
    private String itemId;

    /** 是否为成人用品 */
    private Boolean adultGoods;

    /** isManualChangeSpec */
    private Boolean isManualChangeSpec;

    /** 商品在店铺的top排行榜索引 */
    private Long topItemsSize;

    /** 商品在店铺的top排行榜标题 */
    private String topItemsTitle;

    /** 商品在店铺的top排行榜标题的解释 */
    private String topItemsTitleBrief;

    /** 商品在店铺的top排行榜的标签 */
    private String topItemsTag;

    /** 商品在店铺的top排行榜相关文本 */
    private String subTopItemsTitleUserText;

    /** 商品所在店铺的top排行榜类型 */
    private Long topItemsType;

    /** skuid列表 */
    private JsonNode iterationSku;

    /** 衣服适配评论文本 */
    private String clothFitReviewText;

    /** 衣服适配评论文本 */
    private String popupClothFitReviewText;

    /** 衣服适配的评论信息列表 */
    private JsonNode clothFitReviewInfoList;

    /** 畅销信息模块 */
    private JsonNode bestSellerModule;

    /** 商品评论标签信息 */
    private JsonNode pageLabelsInfo;

    /** 商品轮播图列表 */
    private JsonNode gallery;

    /** 商品属性 */
    private JsonNode goodsProperty;

    /** goodsPropertyPrefer */
    private JsonNode goodsPropertyPrefer;

    /** bannerList */
    private JsonNode bannerList;

    /** detailList */
    private JsonNode detailList;

    /** customImageList */
    private JsonNode customImageList;

    /** rows */
    private Long rows;

    /** statusExplain */
    private String statusExplain;

    /** checkQuantity */
    private Long checkQuantity;

    /** 尺码指南 */
    private JsonNode sizeGuide;

    /** 商品的skc信息列表 */
    private JsonNode skcList;

    /** 规格id列表 */
    private String specIds;

    /** sku信息列表 */
    private JsonNode skuList;

    /** 尺码表信息 */
    private JsonNode sizeSpecModule;

    /** 规格相关的信息 */
    private JsonNode specCustom;

    /** 商品详情图列表(展开) */
    private JsonNode productDetailFlatList;

    /** 商品详情图信息 */
    private JsonNode productDetail;

    /** ext */
    private JsonNode ext;

    /** extendFields */
    private JsonNode extendFields;

    /** 活动信息 */
    private JsonNode activityInfo;

    /** 展示结束时间 */
    private Long displayEndTime;

    /** displayEndTimePercent */
    private Long displayEndTimePercent;

    /** displayEndTimePercentImg */
    private String displayEndTimePercentImg;

    /** selectedSpecIds */
    private JsonNode selectedSpecIds;

    /** 快速预览信息 */
    private JsonNode quickLook;

    /** 类目信息列表 */
    private JsonNode crumbOptList;

    /** 商品标签信息 */
    private JsonNode tagsInfo;

    /** 店铺权益标签 */
    private Long tagCode;

    /** pRec */
    private JsonNode pRec;

    /** 浏览器userAgent头 */
    private String userAgent;

    /** 数据来源url */
    private String fromUrl;

    /** 服务器时间戳 */
    private String serverTime;

    /** 时区 */
    private String timezone;

}
