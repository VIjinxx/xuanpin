package com.lingyun.business.keywordSearch.more.category.pojo;

import lombok.Data;

/**
 * ODS层数据对象 - 关键词搜索查看更多分类数据
 */
@Data
public class ODSKeyMoreCategoryRecord {
    private String date;

    /** 类目id */
    private String optId;

    /** 类目采集站点 */
    private String siteId;

    /** 商家后台类目id */
    private String catId;

    /** 商家后台类目的多层级id信息 */
    private String catInfo;

    /** 类目名 */
    private String optName;

    /** 类目标题 */
    private String title;

    /** 类目标签 */
    private String tagDesc;

    /** 类目类型 */
    private String optType;

    /** 类目层级 */
    private String optLevel;

    /** 类目图片 */
    private String imageUrl;

    /** 模式 */
    private String pattern;

    /** 是否禁止重复 */
    private String disableDup;

    /** 气泡信息 */
    private String bubbleInfo;

    /** 优先级 */
    private String priority;

    /** seo优化链接 */
    private String seoLinkUrl;

    /** 标签类型 */
    private String tabType;

    /** 父类目id */
    private String parentId;

    /** 分享链接 */
    private String shareUrl;

    /** 类目链接 */
    private String linkUrl;

    /** 类目链接 */
    private String href;

    /** 是否特色 */
    private String isFeatured;

    /** 推荐信息 */
    private String pRec;

    /** 浏览器UserAgent头 */
    private String userAgent;

    /** 数据来源URL */
    private String fromUrl;

    /** 服务端时间戳 */
    private String serverTime;

    /** 时区 */
    private String timezone;

    /** 语言 */
    private String lang;

    /** 选择的语言 */
    private String selectedLang;
}
