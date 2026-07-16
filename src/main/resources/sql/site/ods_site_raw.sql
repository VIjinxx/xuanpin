DROP TABLE IF EXISTS ods_site_raw;

CREATE TABLE IF NOT EXISTS ods_site_raw (
    `date` DATE NOT NULL COMMENT "数据采集日期",
    `siteId` INT NOT NULL COMMENT "站点id",
    `regionId` INT COMMENT "地区id",
    `regionName` VARCHAR(255) COMMENT "地区名称",
    `regionShortNamelinkUrl` VARCHAR(100) COMMENT "地区名称缩写",
    `lang` VARCHAR(50) COMMENT "语言",
    `phoneCode` VARCHAR(20) COMMENT "电话号码最开始的数字",
    `selectedLang` VARCHAR(50) COMMENT "选择的语言",
    `selectedCurrency` VARCHAR(20) COMMENT "选择的货币单位",
    `supportedLangList` JSON COMMENT "支持的语言列表",
    `supportedCurrencyList` JSON COMMENT "支持的货币列表",
    `regionImageUrl` VARCHAR(1024) COMMENT "站点国家图标URL",
    `userAgent` VARCHAR(1024) COMMENT "浏览器userAgent头",
    `fromUrl` STRING COMMENT "数据来源url",
    `serverTime` BIGINT COMMENT "服务器时间戳(毫秒)",
    `timezone` VARCHAR(100) COMMENT "时区"
) ENGINE=OLAP
DUPLICATE KEY(`date`, `siteId`)
COMMENT "站点原始信息记录表"
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(`siteId`) BUCKETS 16
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-2147483648",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16",
    "compression" = "ZSTD"
);
