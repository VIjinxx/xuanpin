DROP TABLE IF EXISTS dwd_category_base;
CREATE TABLE IF NOT EXISTS dwd_category_base (
    -- 主键字段
    date DATE COMMENT 'temu前端类目信息统计日期',
    optId INT COMMENT '类目id',
    siteId INT COMMENT '类目采集站点',

    -- 类目基础信息
    lang VARCHAR(32) COMMENT '语言',
    catId VARCHAR(64) COMMENT '商家后台类目id',
    optName VARCHAR(512) COMMENT '类目名',
    title VARCHAR(512) COMMENT '类目标题',
    tagDesc VARCHAR(512) COMMENT '类目标签',
    optType INT COMMENT '类目类型',
    optLevel INT COMMENT '类目层级',
    imageUrl VARCHAR(512) COMMENT '类目图片',
    disableDup BOOLEAN COMMENT '是否禁止重复',
    priority INT COMMENT '优先级',
    seoLinkUrl VARCHAR(1024) COMMENT 'seo优化链接',
    parentId INT COMMENT '父类目id',
    shareUrl VARCHAR(512) COMMENT '分享链接',
    linkUrl VARCHAR(1024) COMMENT '类目链接',
    href VARCHAR(1024) COMMENT '类目链接',
    isFeatured BOOLEAN COMMENT '是否为特色类目'
)
UNIQUE KEY(date, optId, siteId)
COMMENT '类目基础明细表'
PARTITION BY RANGE(date) ()
DISTRIBUTED BY HASH(optId) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-2147483648",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16"
);
