DROP TABLE IF EXISTS dwd_site_base;

CREATE TABLE IF NOT EXISTS dwd_site_base (
    `siteId` INT NOT NULL COMMENT "站点id",
    `date` DATE NOT NULL COMMENT "站点信息统计日期",
    `regionId` INT NOT NULL COMMENT "地区id",
    `regionName` VARCHAR(255) COMMENT "地区名称",
    `lang` VARCHAR(50) COMMENT "语言",
    `regionShortNamelinkUrl` VARCHAR(50) COMMENT "地区名称缩写",
    `phoneCode` VARCHAR(20) COMMENT "电话号码最开始的数字",
    `selectedLang` VARCHAR(50) COMMENT "选择的语言",
    `selectedCurrency` VARCHAR(20) COMMENT "选择的货币单位",
    `supportedLangList` JSON COMMENT "支持的语言列表",
    `supportedCurrencyList` JSON COMMENT "支持的货币列表",
    `regionImageUrl` VARCHAR(500) COMMENT "站点国家图标",
    `userAgent` VARCHAR(1024) COMMENT "浏览器userAgent头",
    `fromUrl` STRING COMMENT "数据来源url",
    `serverTime` BIGINT COMMENT "服务器时间戳",
    `timezone` VARCHAR(100) COMMENT "时区"
) ENGINE=OLAP
UNIQUE KEY(`siteId`, `date`, `regionId`)
COMMENT "站点信息基础明细表"
PARTITION BY RANGE(`date`) (
    FROM ("2024-03-01") TO ("2025-03-01") INTERVAL 1 DAY
)
DISTRIBUTED BY HASH(`siteId`) BUCKETS 16
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-2147483648",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16",
    "enable_unique_key_merge_on_write" = "true"
);
