CREATE TABLE IF NOT EXISTS dwd_review_base (
    `reviewId` VARCHAR(64) NOT NULL COMMENT '评论id',
    `siteId` INT NOT NULL COMMENT '评论采集站点',
    `date` DATE NOT NULL COMMENT '评论信息统计日期',
    `lang` VARCHAR(32) NULL COMMENT '语言',
    `goodsId` VARCHAR(64) NULL COMMENT '商品id',
    `skuId` VARCHAR(64) NULL COMMENT 'sku id',
    `comment` STRING NULL COMMENT '评论内容',
    `score` INT NULL COMMENT '评论分数',
    `specs` JSON NULL COMMENT 'sku规格列表',
    `time` BIGINT NULL COMMENT '时间戳(秒)',
    `concatTimeLang` STRING NULL COMMENT '发表时间描述',
    `concatRichText` JSON NULL COMMENT '发表富文本描述',
    `avatar` STRING NULL COMMENT '用户头像链接',
    `name` STRING NULL COMMENT '用户昵称',
    `profileLinkUrl` STRING NULL COMMENT '用户主页链接',
    `pictures` JSON NULL COMMENT '图片信息列表',
    `opList` JSON NULL COMMENT '操作列表',
    `viewMoreList` JSON NULL COMMENT '查看更多操作列表',
    `reviewLang` JSON NULL COMMENT '评论语言信息',
    `list` JSON NULL COMMENT '图片视频列表',
    `inBlacklist` BOOLEAN NULL COMMENT '是否在黑名单中'
)
UNIQUE KEY(`reviewId`, `siteId`, `date`)
COMMENT '评论基础明细表'
PARTITION BY RANGE(`date`) (
    FROM ("2026-05-01") TO ("2026-06-01") INTERVAL 1 DAY
)
DISTRIBUTED BY HASH(`reviewId`) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "enable_unique_key_merge_on_write" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-2147483648",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16"
);
