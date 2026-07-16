DROP TABLE IF EXISTS dws_category_daily;
CREATE TABLE IF NOT EXISTS dws_category_daily (
    `optId`             INT             NOT NULL COMMENT '类目id',
    `siteId`            INT             NOT NULL COMMENT '类目采集站点',
    `date`              DATE            NOT NULL COMMENT '统计日期',
    `parentId`          INT             COMMENT '父类目id',
    `optNameEnglish`    VARCHAR(512)    COMMENT '类目名英文',
    `optNameChinese`    VARCHAR(512)    COMMENT '类目名中文',
    `imageUrl`          STRING          COMMENT '类目图片链接',
    `seoLinkUrl`        STRING          COMMENT 'seo优化链接',
    `linkUrl`           STRING          COMMENT '类目链接',
    `shareUrl`          STRING          COMMENT '类目分享链接',
    `optType`           INT             COMMENT '类目类型',
    `goodsNum`          BIGINT          COMMENT '类目下的商品数量',
    `isFeatured`        BOOLEAN         COMMENT '是否为特色类目',
    `rank`              INT             COMMENT '类目排名',
    `totalSalesVolume`  BIGINT          COMMENT '总销量',
    `dailySalesVolume`  BIGINT          COMMENT '日销量',
    `totalSalesAmount`  DECIMAL(20,2)   COMMENT '总销售额',
    `totalGoodsNum`     BIGINT          COMMENT '商品总数',
    `dailyGoodsNum`     BIGINT          COMMENT '日商品数',
    `halfPipelineGoodsNum` BIGINT       COMMENT '半托管商品数',
    `totalStoreNum`     BIGINT          COMMENT '店铺总数',
    `dailyStoreNum`     BIGINT          COMMENT '日店铺数',
    `avgCustomerPrice`  DECIMAL(20,2)   COMMENT '平均客单价',
    `dailySalesVolumeChange` DECIMAL(18,4) COMMENT '日销量变化（环比/同比）'
)
UNIQUE KEY(`optId`, `siteId`, `date`)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(`optId`, `siteId`) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-2147483648",
    "dynamic_partition.create_history_partition" = "false",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10"
);
