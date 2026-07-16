package com.lingyun.business.common.sink;

/**
 * Doris表配置枚举
 * 统一管理所有ODS/DWD表名配置
 *
 * @author wxx
 */
public enum DorisTableConfig {

    // Category 类目表
    ODS_CATEGORY("ods_category_raw", "ODS类目原始表"),
    DWD_CATEGORY("dwd_category_base", "DWD类目基础表"),

    // Goods 商品表
    ODS_GOODS("ods_goods_raw", "ODS商品原始表"),
    DWD_GOODS("dwd_goods_base", "DWD商品基础表"),

    // GeekbiGoods 外部数据源商品表
    ODS_GEEKBI_GOODS("ods_geekbi_goods_raw", "ODS外部数据源商品原始表"),
    DWD_GEEKBI_GOODS("dwd_geekbi_goods_daily", "DWD外部数据源商品日明细表"),

    // Site 站点表
    ODS_SITE("ods_site_raw", "ODS站点原始表"),
    DWD_SITE("dwd_site_base", "DWD站点基础表"),

    // Mall 店铺表
    ODS_MALLS("ods_malls_raw", "ODS店铺原始表"),
    DWD_MALLS("dwd_malls_base", "DWD店铺基础表"),

    // Review 评论表
    ODS_REVIEW("ods_review_raw", "ODS评论原始表"),
    DWD_REVIEW("dwd_review_base", "DWD评论基础表");

    private final String tableName;
    private final String description;

    DorisTableConfig(String tableName, String description) {
        this.tableName = tableName;
        this.description = description;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDescription() {
        return description;
    }

    /**
     * 根据表名查找配置
     *
     * @param tableName 表名
     * @return 对应的配置，找不到返回null
     */
    public static DorisTableConfig findByTableName(String tableName) {
        for (DorisTableConfig config : values()) {
            if (config.tableName.equals(tableName)) {
                return config;
            }
        }
        return null;
    }
}
