package com.lingyun.business.common.model.review;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

/**
 * 通用 DWD 层评论记录。
 * 对应表: dwd_review_base
 */
@Data
public class DWDReviewRecord {
    /** 评论id */
    private String reviewId;

    /** 评论采集站点 */
    private Long siteId;

    /** 评论信息统计日期 */
    private String date;

    /** 语言 */
    private String lang;

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

    /** 发表时间描述 */
    private String concatTimeLang;

    /** 发表富文本描述 */
    private JsonNode concatRichText;

    /** 用户头像链接 */
    private String avatar;

    /** 用户昵称 */
    private String name;

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

    /** 是否在黑名单中 */
    private Boolean inBlacklist;
}
