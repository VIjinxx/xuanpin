CREATE TABLE IF NOT EXISTS lingyun.ods_geekbi_goods_raw (
    `goodsId` BIGINT NOT NULL COMMENT '商品id，从data.goods.goodsId提取',
    `siteId` INT NOT NULL COMMENT '站点id，从data.site.id提取',
    `date` DATE NOT NULL COMMENT '数据采集日期',
    `rankings` JSON NULL COMMENT '排名信息',
    `site` JSON NULL COMMENT '站点信息',
    `stats` JSON NULL COMMENT '区域销售占比信息',
    `goods` JSON NULL COMMENT '商品当前快照',
    `history` JSON NULL COMMENT '商品历史每日明细'
)
DUPLICATE KEY(`goodsId`, `siteId`, `date`)
COMMENT '外部数据源商品原始数据表'
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(`goodsId`) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-2147483648",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16"
);
