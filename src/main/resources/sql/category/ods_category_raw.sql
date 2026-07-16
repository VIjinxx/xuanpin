DROP TABLE IF EXISTS ods_category_raw;
CREATE TABLE IF NOT EXISTS ods_category_raw (
    -- 分区字段
    `date` DATE COMMENT '数据日期',
    -- 类目基础信息
    optId INT COMMENT '类目id',
    siteId INT COMMENT '类目采集站点',
    catId VARCHAR(64) COMMENT '商家后台类目id',
    catInfo JSON COMMENT '商家后台类目的多层级id信息',
    optName VARCHAR(512) COMMENT '类目名',
    title VARCHAR(512) COMMENT '类目标题',
    tagDesc VARCHAR(512) COMMENT '类目标签',
    optType INT COMMENT '类目类型',
    optLevel INT COMMENT '类目层级',
    imageUrl VARCHAR(512) COMMENT '类目图片',
    disableDup BOOLEAN COMMENT '是否禁止重复',
    bubbleInfo JSON COMMENT '',
    pattern INT COMMENT '',
    priority INT COMMENT '优先级',
    seoLinkUrl VARCHAR(1024) COMMENT 'seo优化链接',
    tabType INT COMMENT '',
    parentId INT COMMENT '父类目id',
    shareUrl VARCHAR(512) COMMENT '分享链接',
    linkUrl VARCHAR(1024) COMMENT '类目链接',
    href VARCHAR(1024) COMMENT '类目链接',
    isFeatured BOOLEAN COMMENT '',
    pRec JSON COMMENT '',

    -- 来源信息
    userAgent VARCHAR(512) COMMENT '浏览器userAgent头',
    fromUrl VARCHAR(1024) COMMENT '数据来源url',
    serverTime VARCHAR(64) COMMENT '服务器时间戳',
    timezone VARCHAR(64) COMMENT '时区',
    lang VARCHAR(32) COMMENT '语言',
    selectedLang VARCHAR(32) COMMENT '选择的语言'
)
DUPLICATE KEY(`date`, optId, siteId)
COMMENT '类目原始采集表'
PARTITION BY RANGE(`date`) ()
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
