DROP TABLE IF EXISTS dwd_goods_base;
CREATE TABLE IF NOT EXISTS dwd_goods_base (
    `goodsId` BIGINT COMMENT '商品id',
    `siteId` BIGINT COMMENT '商品所在站点，全托管商品不区分站点，该字段直接为空，半托管商品区分站点，该字段不为空',
    `date` DATE COMMENT '商品信息统计日期',
    `lang` STRING COMMENT '语言',
    `selectedCurrency` STRING COMMENT '选择的货币单位',
    `mallId` STRING COMMENT '商品所属店铺id',
    `optId` BIGINT COMMENT '前端类目id',
    `catId` BIGINT COMMENT '商家后台类目id',
    `title` STRING COMMENT '商品标题',
    `linkUrl` STRING COMMENT '商品链接',
    `salesNum` BIGINT COMMENT '销售数量',
    `isOnsale` BIGINT COMMENT '是否在售',
    `isShowMarketPrice` BOOLEAN COMMENT '是否展示原价',
    `reviewNum` BIGINT COMMENT '评论数量',
    `goodsScore` DOUBLE COMMENT '商品评分',
    `minOnSalePrice` BIGINT COMMENT '销售价格最低值(单位分)',
    `maxOnSalePrice` BIGINT COMMENT '销售价格最高值(单位分)',
    `priceInfo` JSON COMMENT '价格信息',
    `image` JSON COMMENT '图片信息',
    `brandCard` JSON COMMENT '品牌卡片',
    `guideFile` JSON COMMENT '指导书文件',
    `imageUrl` STRING COMMENT '商品图片链接',
    `thumbUrl` STRING COMMENT '略缩图链接',
    `video` JSON COMMENT '商品视频信息',
    `visible` BOOLEAN COMMENT '商品是否可见',
    `pageAlt` STRING COMMENT '商品页面描述信息',
    `semiManaged` BOOLEAN COMMENT '是否半托管',
    `soldQuantityPercent` BIGINT COMMENT '销量占库存百分比(存疑)',
    `wareHouseType` BIGINT COMMENT '仓库类型',
    `isLocalGoods` BIGINT COMMENT '是否为本地商品',
    `adultGoods` BOOLEAN COMMENT '是否为成人用品',
    `gallery` JSON COMMENT '商品轮播图列表',
    `goodsProperty` JSON COMMENT '商品属性',
    `sizeGuide` JSON COMMENT '尺码指南',
    `skcList` JSON COMMENT '商品的skc信息列表',
    `specIds` STRING COMMENT '规格id列表',
    `skuList` JSON COMMENT 'sku信息列表',
    `trackingKey` STRING COMMENT '关键词',
    `type` BIGINT COMMENT '类型',
    `supportPromotion` BOOLEAN COMMENT '是否支持促销',
    `sizeSpecModule` JSON COMMENT '尺码表信息',
    `specCustom` JSON COMMENT '规格相关的信息',
    `productDetailFlatList` JSON COMMENT '商品详情图列表(展开)',
    `productDetail` JSON COMMENT '商品详情图信息',
    `displayEndTime` BIGINT COMMENT '展示结束时间',
    `crumbOptList` JSON COMMENT '类目信息列表'
)
UNIQUE KEY(`goodsId`, `siteId`, `date`)
COMMENT '商品基础明细表'
PARTITION BY RANGE(`date`) (
    FROM ("2026-05-01") TO ("2026-06-01") INTERVAL 1 DAY
)
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
