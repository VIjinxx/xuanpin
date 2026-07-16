CREATE TABLE IF NOT EXISTS ods_review_raw (
    `date` DATE NOT NULL COMMENT '数据采集日期',
    `reviewId` VARCHAR(64) NOT NULL COMMENT '评论id',
    `siteId` INT NOT NULL COMMENT '评论采集站点',
    `goodsId` VARCHAR(64) NULL COMMENT '商品id',
    `skuId` VARCHAR(64) NULL COMMENT 'sku id',
    `comment` STRING NULL COMMENT '评论内容',
    `score` INT NULL COMMENT '评论分数',
    `specs` JSON NULL COMMENT 'sku规格列表',
    `time` BIGINT NULL COMMENT '时间戳(秒)',
    `timeMs` BIGINT NULL COMMENT '时间戳(毫秒)',
    `concatTimeLang` STRING NULL COMMENT '发表时间描述',
    `concatRichText` JSON NULL COMMENT '发表富文本描述',
    `avatar` STRING NULL COMMENT '用户头像链接',
    `name` STRING NULL COMMENT '用户昵称',
    `isOwnReview` BOOLEAN NULL COMMENT '是否为自己的评论',
    `isSimilarReview` BOOLEAN NULL COMMENT '是否为相似评论',
    `profileLinkUrl` STRING NULL COMMENT '用户主页链接',
    `pictures` JSON NULL COMMENT '图片信息列表',
    `opList` JSON NULL COMMENT '操作列表',
    `viewMoreList` JSON NULL COMMENT '查看更多操作列表',
    `reviewLang` JSON NULL COMMENT '评论语言信息',
    `list` JSON NULL COMMENT '图片视频列表',
    `goodsSpecificReviewLevelInfo` JSON NULL COMMENT '评论标签信息',
    `extendParams` JSON NULL COMMENT '扩展参数',
    `inBlacklist` BOOLEAN NULL COMMENT '是否在黑名单中',
    `userAgent` STRING NULL COMMENT '浏览器userAgent头',
    `fromUrl` STRING NULL COMMENT '数据来源url',
    `serverTime` STRING NULL COMMENT '服务器时间戳',
    `timezone` STRING NULL COMMENT '时区',
    `lang` VARCHAR(32) NULL COMMENT '语言',
    `selectedLang` VARCHAR(32) NULL COMMENT '选择的语言'
)
DUPLICATE KEY(`date`, `reviewId`, `siteId`)
COMMENT '评论原始采集表'
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
