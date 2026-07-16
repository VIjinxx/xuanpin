DROP TABLE IF EXISTS lingyun.dwd_malls_base;

CREATE TABLE lingyun.dwd_malls_base (
    mallId BIGINT COMMENT '店铺id',
    siteId INT COMMENT '店铺采集站点',
    `date` DATE COMMENT '店铺信息统计日期',
    lang STRING COMMENT '语言',
    mallName STRING COMMENT '店铺名称',
    reviewNum INT COMMENT '评论数',
    mallLogo STRING COMMENT '店铺logo图片链接',
    goodsNum INT COMMENT '商品数量',
    mallStar DECIMAL(10,1) COMMENT '店铺评分',
    goodsSalesNum BIGINT COMMENT '商品已售数量',
    goodsSalesBrief STRING COMMENT '商品已售简介',
    followerNum BIGINT COMMENT '店铺关注者数量',
    semiManaged BOOLEAN COMMENT '是否为半托管',
    searchUrl STRING COMMENT '搜索链接',
    seoUrl STRING COMMENT 'seo链接',
    mallJumpUrl STRING COMMENT '店铺跳转链接',
    mallReviewLinkUrl STRING COMMENT '店铺评论链接',
    shareInfo_shareTitle STRING COMMENT '店铺分享标题',
    shareInfo_shareImg STRING COMMENT '店铺分享图片',
    shareInfo_shareUrl STRING COMMENT '店铺分享链接',
    scoreNumInfoList JSON COMMENT '评分分布信息列表',
    topShopInfo_topShopText_textData STRING COMMENT '畅销文本',
    topShopInfo_topShopGoToUrl STRING COMMENT '畅销店铺排名跳转链接',
    optList JSON COMMENT '店铺类目信息列表',
    semiManagedMallTags_contents_text STRING COMMENT '半托管信息文本内容',
    semiManagedMallTags_contents_url STRING COMMENT '半托管信息图片链接',
    lastUpdateTime DATETIME COMMENT '源数据最后更新时间戳'
)
UNIQUE KEY(mallId, siteId, `date`)
COMMENT '店铺基础信息表'
PARTITION BY RANGE (`date`) ()
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
