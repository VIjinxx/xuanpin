package com.lingyun.business.common.model.category;

import lombok.Data;

/**
 * 通用 ODS 层类目记录
 * 对应表: ods_category_raw
 */
@Data
public class ODSCategoryRecord {

    /** 数据日期 */
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

    /** 是否禁止重复 */
    private String disableDup;

    /** 气泡信息 */
    private String bubbleInfo;

    /** 模式 */
    private String pattern;

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

