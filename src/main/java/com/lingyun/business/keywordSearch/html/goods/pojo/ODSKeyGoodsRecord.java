package com.lingyun.business.keywordSearch.html.goods.pojo;

import lombok.Data;

@Data
public class ODSKeyGoodsRecord {

    /** 商品ID */
    private String goodsId;

    /** 店铺ID */
    private String mallId;

    /** 当前SKU ID */
    private String currentSkuId;

    /** 默认SKU ID */
    private String defaultSkuId;

    /** 站点ID */
    private String siteId;

    /** 选择的语言 */
    private String selectedLang;

    /** 语言 */
    private String lang;

    /** 选择的货币 */
    private String selectedCurrency;

    /** 运营ID */
    private String optId;

    /** 运营类型 */
    private String optType;

    /** 运营等级 */
    private String optLevel;

    /** 类目ID */
    private String catId;

    /** 类目信息 */
    private String catInfo;

    /** 标题 */
    private String title;

    /** 商品名称 */
    private String goodsName;

    /** 链接URL */
    private String linkUrl;

    /** 活动类型 */
    private String activityType;

    /** 侧边销售提示 */
    private String sideSalesTip;

    /** 销售提示 */
    private String salesTip;

    /** 销售提示文本 */
    private String salesTipText;

    /** 销售提示文本列表 */
    private String salesTipTextList;

    /** 是否在售 */
    private String isOnsale;

    /** 是否显示市场价 */
    private String isShowMarketPrice;

    /** 是否SKC */
    private String isSkc;

    /** 数量 */
    private String qty;

    /** 状态 */
    private String status;

    /** 缺货订阅状态 */
    private String soldOutSubscribeStatus;

    /** 销量 */
    private String salesNum;

    /** 已售数量 */
    private String soldQuantity;

    /** 评论数 */
    private String reviewNum;

    /** 评论数提示 */
    private String commentNumTips;

    /** 商品评分 */
    private String goodsScore;

    /** 是否显示评分 */
    private String showScore;

    /** 隐藏评论 */
    private String hiddenComment;

    /** SEO链接URL */
    private String seoLinkUrl;

    /** 默认选中规格 */
    private String defaultSelectSpec;

    /** 最低在售价格 */
    private String minOnSalePrice;

    /** 最高在售价格 */
    private String maxOnSalePrice;

    /** 价格信息 */
    private String priceInfo;

    /** 图片 */
    private String image;

    /** 品牌卡片 */
    private String brandCard;

    /** 最后更新时间戳 */
    private String lastUpdateTime;

    /** 分区日期 */
    private String date;
}
