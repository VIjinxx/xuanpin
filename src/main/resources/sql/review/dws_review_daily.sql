DROP TABLE IF EXISTS dws_review_daily;

CREATE TABLE IF NOT EXISTS dws_review_daily (
    `date` DATE COMMENT '评论信息统计日期',
    `reviewId` VARCHAR(64) COMMENT '评论的唯一标识ID',
    `commentEnglish` STRING COMMENT '用户评论英文内容',
    `commentChinese` STRING COMMENT '用户评论中文内容',
    `time` BIGINT COMMENT '评论的时间戳',
    `name` STRING COMMENT '评论用户昵称',
    `avatar` STRING COMMENT '评论用户头像',
    `profileLinkUrl` STRING COMMENT '用户资料页的链接',
    `score` INT COMMENT '用户评分',
    `goodsId` VARCHAR(64) COMMENT '用户评论的商品id',
    `skuId` VARCHAR(64) COMMENT '用户评论的sku id',
    `imageOrVideoList` JSON COMMENT '评论中包含的图片/视频列表',
    `specs` JSON COMMENT '商品规格列表'
)
UNIQUE KEY(`date`, `reviewId`)
COMMENT '评论日聚合表'
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(`reviewId`) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-2147483648",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16"
);
