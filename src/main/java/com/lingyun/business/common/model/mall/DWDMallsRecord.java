package com.lingyun.business.common.model.mall;

import lombok.Data;

/**
 * 通用 DWD 层店铺记录
 * 对应表: dwd_malls_base
 */
@Data
public class DWDMallsRecord {
    /** 店铺ID */
    private String mallId;

    /** 站点ID */
    private String siteId;

    /** 当前语言 */
    private String lang;

    /** 分区日期 */
    private String date;

    /** 店铺名称 */
    private String mallName;

    /** 评论数 */
    private String reviewNum;

    /** 店铺 Logo 图片链接 */
    private String mallLogo;

    /** 商品数量 */
    private String goodsNum;

    /** 店铺评分 */
    private String mallStar;

    /** 商品已售数量 */
    private Long goodsSalesNum;

    /** 商品已售简介文本 */
    private String goodsSalesBrief;

    /** 店铺关注者数量 */
    private String followerNum;

    /** 是否半托管 */
    private Boolean semiManaged;

    /** 搜索链接 */
    private String searchUrl;

    /** SEO 链接 */
    private String seoUrl;

    /** 店铺跳转链接 */
    private String mallJumpUrl;

    /** 店铺评论跳转链接 */
    private String mallReviewLinkUrl;

    /** 分享标题 */
    private String shareInfo_shareTitle;

    /** 分享图片 */
    private String shareInfo_shareImg;

    /** 分享链接 */
    private String shareInfo_shareUrl;

    /** 评分分布信息列表 */
    private String scoreNumInfoList;

    /** 畅销店铺文案 */
    private String topShopInfo_topShopText_textData;

    /** 畅销店铺跳转链接 */
    private String topShopInfo_topShopGoToUrl;

    /** 店铺类目信息列表 */
    private String optList;

    /** 半托管标签文案 */
    private String semiManagedMallTags_contents_text;

    /** 半托管标签链接 */
    private String semiManagedMallTags_contents_url;

    /** 最后更新时间戳 */
    private String lastUpdateTime;
}
