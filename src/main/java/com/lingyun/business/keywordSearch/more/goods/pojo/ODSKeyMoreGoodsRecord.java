package com.lingyun.business.keywordSearch.more.goods.pojo;

import lombok.Data;

/**
 * ODS层数据对象 - 关键词搜索查看更多商品数据
 */
@Data
public class ODSKeyMoreGoodsRecord {

    /** 商品ID */
    private String goodsId;

    /** 店铺ID */
    private String mallId;

    /** 运营ID */
    private String optId;

    /** 运营类型 */
    private String optType;

    /** 标题 */
    private String title;

    /** 链接URL */
    private String linkUrl;

    /** 销售提示 */
    private String salesTip;

    /** 销量 */
    private String salesNum;

    /** 评论数提示 */
    private String commentNumTips;

    /** 商品评分 */
    private String goodsScore;

    /** 隐藏评论 */
    private String hiddenComment;

    /** SEO链接URL */
    private String seoLinkUrl;

    /** 高清缩略图URL */
    private String hdThumbUrl;

    /** 缩略图URL */
    private String thumbUrl;

    /** 商品图片 */
    private String goodsImage;

    /** 图片URL */
    private String imageUrl;

    /** 长缩略图URL */
    private String longThumbUrl;

    /** 查询相关性得分 */
    private String queryReleScore;

    /** 可见性 */
    private String visible;

    /** 页面Alt */
    private String pageAlt;

    /** 商品类型 */
    private String itemType;

    /** 已售数量百分比 */
    private String soldQuantityPercent;

    /** 仓库类型 */
    private String wareHouseType;

    /** 成人商品 */
    private String adultGoods;

    /** 展示结束时间 */
    private String displayEndTime;

    /** 展示结束时间百分比 */
    private String displayEndTimePercent;

    /** 选中的规格ID列表 */
    private String selectedSpecIds;

    /** 浏览器UserAgent头 */
    private String userAgent;

    /** 数据来源URL */
    private String fromUrl;

    /** 服务端时间戳 */
    private String serverTime;

    /** SKC列表 */
    private String skcList;

    /** 标题头部标签 */
    private String titleHeaderTags;

    /** 商品标签 */
    private String goodsTags;

    /** 店铺标签 */
    private String mallTag;

    /** 广告标签 */
    private String adTags;

    /** 今日标签 */
    private String todayTags;

    /** 价格信息 */
    private String priceInfo;

    /** 图片 */
    private String image;

    /** 视频 */
    private String video;

    /** 优惠文本 */
    private String benefitText;

    /** 扩展字段 */
    private String extendFields;

    /** 快速查看 */
    private String quickLook;

    /** 推荐记录 */
    private String pRec;

    /** 最后更新时间戳 */
    private String lastUpdateTime;

    /** 分区日期 */
    private String date;
}