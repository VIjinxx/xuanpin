package com.lingyun.business.common.model.site;

import lombok.Data;

/**
 * 通用 DWD 层站点记录
 * 对应表: dwd_site_base
 */
@Data
public class DWDSiteRecord {
    /** 站点ID */
    private String siteId;

    /** 区域ID */
    private String regionId;

    /** 区域名称 */
    private String regionName;

    /** 区域短名称链接URL */
    private String regionShortNamelinkUrl;

    /** 电话区号 */
    private String phoneCode;

    /** 选择的语言 */
    private String selectedLang;

    /** 选择的货币 */
    private String selectedCurrency;

    /** 支持的语言列表 */
    private String supportedLangList;

    /** 支持的货币列表 */
    private String supportedCurrencyList;

    /** 区域图片URL */
    private String regionImageUrl;

    /** 浏览器 UserAgent 头 */
    private String userAgent;

    /** 服务端时间戳 */
    private String serverTime;

    /** 时区 */
    private String timezone;

    /** 数据来源 URL */
    private String fromUrl;

    /** 当前语言 */
    private String lang;

    /** 分区日期 */
    private String date;

    /** 最后更新时间戳 */
    private String lastUpdateTime;
}

