package com.lingyun.business.common.model.goods;

/** 商品设计表逻辑类型到 Doris 物理类型的映射。 */
public enum GoodsFieldType {
    STRING("STRING"),
    INTEGER("BIGINT"),
    FLOAT("DOUBLE"),
    BOOLEAN("BOOLEAN"),
    DATE("DATE"),
    JSON("JSON");

    private final String sqlType;

    GoodsFieldType(String sqlType) { this.sqlType = sqlType; }

    public String getSqlType() { return sqlType; }
}
