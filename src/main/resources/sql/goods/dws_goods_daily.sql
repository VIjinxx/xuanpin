DROP TABLE IF EXISTS dws_goods_daily;
CREATE TABLE IF NOT EXISTS dws_goods_daily (
    `goodsId` BIGINT COMMENT '商品id',
    `siteId` BIGINT COMMENT '商品所在站点',
    `date` DATE COMMENT '商品信息统计日期',
    `goodsTitleEnglish` STRING COMMENT '商品英文标题',
    `goodsTitleChinese` STRING COMMENT '商品中文标题',
    `mallId` STRING COMMENT '商品所属店铺id',
    `mainImageUrl` STRING COMMENT '商品主图',
    `thumbUrl` TEXT NULL COMMENT '略缩图链接',
    `carouselImageUrls` JSON COMMENT '商品轮播图列表',
    `optId` BIGINT COMMENT '商品的temu前端的类目id',
    `catId` BIGINT COMMENT '商品的temu后端的类目id',
    `isAdultGoods` BOOLEAN COMMENT '是否为成人用品',
    `videoInfo` JSON COMMENT '商品视频信息',
    `propertyList` JSON COMMENT '属性列表',
    `salesNum` BIGINT COMMENT '当前销售数量',
    `currency` STRING COMMENT '商品售卖货币类型',
    `minOnSalePrice` BIGINT COMMENT '商品当前售卖规格最低价格',
    `maxOnSalePrice` BIGINT COMMENT '商品当前售卖规格最高价格',
    `score` DOUBLE COMMENT '商品当前评分',
    `reviewNum` BIGINT COMMENT '商品当前评论数',
    `stockStatus` BIGINT COMMENT '商品当前库存状态',
    `stockQuantity` BIGINT COMMENT '商品当前库存数量',
    `addToRegionTime` DATE COMMENT '在该站点上架时间',
    `skuInfoList` JSON COMMENT 'sku信息',
    `firstAddTime` DATE COMMENT '该商品数据第一次被添加时间',
    `lastUpdateTime` DATE COMMENT '该商品数据上一次更新时间'
)
UNIQUE KEY(`goodsId`, `siteId`, `date`)
COMMENT '商品日聚合表'
DISTRIBUTED BY HASH(`goodsId`, `siteId`, `date`) BUCKETS 16
PROPERTIES (
    "replication_num" = "3"
);
