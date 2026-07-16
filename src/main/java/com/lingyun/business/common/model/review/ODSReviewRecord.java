package com.lingyun.business.common.model.review;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

/**
 * 通用 ODS 层评论记录。
 * 对应表: ods_review_raw
 */
@Data
public class ODSReviewRecord {
    /** 数据采集日期 */
    private String date;

    /** 评论id */
    private String reviewId;

    /** 评论采集站点 */
    private Long siteId;

    /** 商品id */
    private String goodsId;

    /** sku id */
    private String skuId;

    /** 评论内容 */
    private String comment;

    /** 评论分数 */
    private Long score;

    /** sku规格列表 */
    private JsonNode specs;

    /** 时间戳(秒) */
    private Long time;

    /** 时间戳(毫秒) */
    private Long timeMs;

    /** 发表时间描述 */
    private String concatTimeLang;

    /** 发表富文本描述 */
    private JsonNode concatRichText;

    /** 用户头像链接 */
    private String avatar;

    /** 用户昵称 */
    private String name;

    /** 是否为自己的评论 */
    private Boolean isOwnReview;

    /** 是否为相似评论 */
    private Boolean isSimilarReview;

    /** 用户主页链接 */
    private String profileLinkUrl;

    /** 图片信息列表 */
    private JsonNode pictures;

    /** 操作列表 */
    private JsonNode opList;

    /** 查看更多操作列表 */
    private JsonNode viewMoreList;

    /** 评论语言信息 */
    private JsonNode reviewLang;

    /** 图片视频列表 */
    private JsonNode list;

    /** 评论标签信息 */
    private JsonNode goodsSpecificReviewLevelInfo;

    /** 扩展参数 */
    private JsonNode extendParams;

    /** 是否在黑名单中 */
    private Boolean inBlacklist;

    /** 浏览器userAgent头 */
    private String userAgent;

    /** 数据来源url */
    private String fromUrl;

    /** 服务器时间戳 */
    private String serverTime;

    /** 时区 */
    private String timezone;

    /** 语言 */
    private String lang;

    /** 选择的语言 */
    private String selectedLang;
}
