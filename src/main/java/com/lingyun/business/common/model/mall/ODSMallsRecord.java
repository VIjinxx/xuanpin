package com.lingyun.business.common.model.mall;

import lombok.Data;

/**
 * 通用 ODS 层店铺记录
 * 对应表: ods_malls_raw
 */
@Data
public class ODSMallsRecord {

    /** 店铺id */
    private String mallId;

    /** 店铺名称 */
    private String mallName;

    /** 评论数 */
    private String reviewNum;

    /** 评论数字符串 */
    private String reviewNumStr;

    /** 店铺 logo 图片链接 */
    private String mallLogo;

    /** 组件配图 CDN 资源地址 */
    private String picUrl;

    /** 商家头像 CDN 在线地址 */
    private String avatar;

    /** 入驻渠道编码 */
    private String appCode;

    /** 商家店铺昵称 */
    private String nickname;

    /** 商家头像 */
    private Boolean isDefaultAvatar;

    /** 商品数量 */
    private String goodsNum;

    /** 商品数量（v2） */
    private String goodsNumV2;

    /** 商品数量（含单位，例如 ["66","商品"]） */
    private String goodsNumUnit;

    /** 商品数量（小写单位） */
    private String goodsNumLowercaseUnit;

    /** 店铺评分 */
    private String mallStar;

    /** 店铺评分字符串 */
    private String mallStarStr;

    /** 缩短星级（一般为0） */
    private String shortenMallStar;

    /** 商品已售数量 */
    private String goodsSalesNum;

    /** 商品已售数量（带单位，例如 ["6,049","已售"]） */
    private String goodsSalesNumUnit;

    /** 商品已售简介文本 */
    private String goodsSalesBrief;

    /** 店铺关注者数量 */
    private String followerNum;

    /** 店铺关注者数量（带单位） */
    private String followerNumUnit;

    /** 是否半托管 */
    private Boolean semiManaged;

    /** 渠道类型 */
    private String channelType;

    /** 搜索链接 */
    private String searchUrl;

    /** SEO 链接 */
    private String seoUrl;

    /** 店铺跳转链接 */
    private String mallJumpUrl;

    /** 店铺分享信息（存储为JSON字符串） */
    private String shareInfo;

    /** 评分分布信息列表（存储为JSON字符串） */
    private String scoreNumInfoList;

    /** 畅销店铺信息（存储为JSON字符串） */
    private String topShopInfo;

    /** 是否展示店铺评论标签 */
    private String mallReviewLabelShow;

    /** 店铺评论跳转链接 */
    private String mallReviewLinkUrl;

    /** 店铺类目信息列表（存储为JSON字符串） */
    private String optList;

    /** 市场价类型 */
    private String marketPriceType;

    /** 单商品数据 */
    private String goodsSimpleInfoList;

    /** 是否为 AI 虚拟评价 */
    private Boolean isAiReview;

    /** 店铺标签信息（存储为JSON字符串） */
    private String mallTags;

    /** 半托管相关标签信息（存储为JSON字符串） */
    private String semiManagedMallTags;

    /** 是否过期 */
    private String isExpire;

    /** 浏览器 UserAgent 头 */
    private String userAgent;

    /** 数据来源 URL */
    private String fromUrl;

    /** 服务端时间戳 */
    private String serverTime;

    /** 时区 */
    private String timezone;

    /** 站点 ID */
    private String siteId;

    /** 当前语言 */
    private String lang;

    /** 用户选择语言 */
    private String selectedLang;

    /** 最后更新时间戳 */
    private String lastUpdateTime;

    /** 数据来源 */
    private String source;

    /** 分区日期 */
    private String date;
}
