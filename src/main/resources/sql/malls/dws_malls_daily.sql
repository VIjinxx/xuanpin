DROP TABLE IF EXISTS lingyun.dws_malls_daily;

CREATE TABLE lingyun.dws_malls_daily (
    mallId BIGINT COMMENT '店铺id',
    siteId INT COMMENT '店铺采集站点',
    `date` DATE COMMENT '统计日期',
    mallName STRING COMMENT '店铺名',
    mallLogoUrl STRING COMMENT '店铺logo链接',
    mallType INT COMMENT '是否半托管：0=非半托管，1=半托管',
    categoryList JSON COMMENT '主营类目列表',
    goodsNum INT COMMENT '店铺当前商品数量',
    goodsSalesNum INT COMMENT '已售商品数量',
    goodsSalesPrice BIGINT COMMENT '已销售总金额',
    mallStar DECIMAL(10,1) COMMENT '评分',
    scorePercentInfo JSON COMMENT '评分分布信息',
    reviewNum INT COMMENT '评论数量',
    followerNum BIGINT COMMENT '关注数量',
    addToRegionTime DATE COMMENT '开店时间',
    lastUpdateTime DATETIME COMMENT '上一次更新时间'
)
UNIQUE KEY(mallId, siteId, `date`)
COMMENT '店铺日粒度汇总表'
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(mallId) BUCKETS 24
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "enable_unique_key_merge_on_write" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-2147483648",
    "dynamic_partition.end" = "7",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "24",
    "dynamic_partition.create_history_partition" = "false",
    "compression" = "zstd"
);
