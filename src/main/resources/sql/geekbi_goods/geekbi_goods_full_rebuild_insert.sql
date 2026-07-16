-- DWS full rebuild insert.
-- This script only inserts rebuilt historical rows. Clear the target DWS table outside this script when needed.
-- Uses validity-interval expansion (LEAD) instead of date-range ROW_NUMBER to reduce memory usage.

INSERT INTO dws_goods_daily
(
    goodsId,
    siteId,
    date,
    goodsTitleEnglish,
    goodsTitleChinese,
    mallId,
    mainImageUrl,
    thumbUrl,
    carouselImageUrls,
    optId,
    catId,
    isAdultGoods,
    videoInfo,
    propertyList,
    salesNum,
    currency,
    minOnSalePrice,
    maxOnSalePrice,
    score,
    reviewNum,
    stockStatus,
    stockQuantity,
    addToRegionTime,
    skuInfoList,
    firstAddTime,
    lastUpdateTime
)
WITH geekbi_dates AS (
    SELECT DISTINCT `date` AS stat_date
    FROM dwd_geekbi_goods_daily
    WHERE `date` IS NOT NULL
),
geekbi_goods_intervals AS (
    SELECT
        dwd.goodsId,
        dwd.siteId,
        dwd.`date`,
        dwd.mallId,
        dwd.goodsName,
        dwd.goodsNameCn,
        dwd.goodsNameEn,
        dwd.thumbnail,
        dwd.thumbnailCn,
        dwd.optId,
        dwd.catId,
        dwd.sold,
        dwd.currency,
        dwd.cents,
        dwd.minPrice,
        dwd.maxPrice,
        dwd.goodsScore,
        dwd.reviewNum,
        dwd.status,
        dwd.quantity,
        dwd.onSaleTime,
        dwd.skuInfo,
        LEAD(dwd.`date`) OVER (
            PARTITION BY dwd.goodsId, dwd.siteId
            ORDER BY dwd.`date`
        ) AS next_date,
        MIN(dwd.`date`) OVER (
            PARTITION BY dwd.goodsId, dwd.siteId
            ORDER BY dwd.`date`
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS firstCollectDate,
        MIN(CAST(dwd.onSaleTime AS DATE)) OVER (
            PARTITION BY dwd.goodsId, dwd.siteId
            ORDER BY dwd.`date`
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS firstOnSaleDate
    FROM dwd_geekbi_goods_daily dwd
    WHERE dwd.goodsId IS NOT NULL
        AND dwd.siteId IS NOT NULL
),
daily_snapshots AS (
    SELECT
        intervals.*,
        dates.stat_date
    FROM geekbi_goods_intervals intervals
    JOIN geekbi_dates dates
        ON dates.stat_date >= intervals.`date`
        AND (intervals.next_date IS NULL OR dates.stat_date < intervals.next_date)
)
SELECT
    daily_snapshots.goodsId,
    daily_snapshots.siteId,
    daily_snapshots.stat_date AS `date`,
    COALESCE(
        NULLIF(daily_snapshots.goodsNameEn, ''),
        NULLIF(daily_snapshots.goodsName, ''),
        NULLIF(daily_snapshots.goodsNameCn, '')
    ) AS goodsTitleEnglish,
    COALESCE(
        NULLIF(daily_snapshots.goodsNameCn, ''),
        NULLIF(daily_snapshots.goodsName, ''),
        NULLIF(daily_snapshots.goodsNameEn, '')
    ) AS goodsTitleChinese,
    daily_snapshots.mallId AS mallId,
    COALESCE(
        NULLIF(daily_snapshots.thumbnail, ''),
        NULLIF(daily_snapshots.thumbnailCn, '')
    ) AS mainImageUrl,
    daily_snapshots.thumbnail AS thumbUrl,
    CASE
        WHEN COALESCE(
            NULLIF(daily_snapshots.thumbnail, ''),
            NULLIF(daily_snapshots.thumbnailCn, '')
        ) IS NOT NULL THEN JSON_ARRAY(COALESCE(
            NULLIF(daily_snapshots.thumbnail, ''),
            NULLIF(daily_snapshots.thumbnailCn, '')
        ))
        ELSE CAST('[]' AS JSON)
    END AS carouselImageUrls,
    daily_snapshots.optId AS optId,
    daily_snapshots.catId AS catId,
    NULL AS isAdultGoods,
    NULL AS videoInfo,
    NULL AS propertyList,
    daily_snapshots.sold AS salesNum,
    daily_snapshots.currency AS currency,
    CASE
        WHEN daily_snapshots.minPrice IS NULL THEN NULL
        ELSE CAST(ROUND(daily_snapshots.minPrice * COALESCE(daily_snapshots.cents, 100), 0) AS BIGINT)
    END AS minOnSalePrice,
    CASE
        WHEN daily_snapshots.maxPrice IS NULL THEN NULL
        ELSE CAST(ROUND(daily_snapshots.maxPrice * COALESCE(daily_snapshots.cents, 100), 0) AS BIGINT)
    END AS maxOnSalePrice,
    CAST(daily_snapshots.goodsScore AS DOUBLE) AS score,
    daily_snapshots.reviewNum AS reviewNum,
    CASE
        WHEN COALESCE(daily_snapshots.status, 0) = 1
            AND COALESCE(daily_snapshots.quantity, 0) > 0 THEN 0
        ELSE 2
    END AS stockStatus,
    daily_snapshots.quantity AS stockQuantity,
    COALESCE(
        CAST(daily_snapshots.onSaleTime AS DATE),
        daily_snapshots.firstOnSaleDate,
        daily_snapshots.firstCollectDate,
        daily_snapshots.stat_date
    ) AS addToRegionTime,
    daily_snapshots.skuInfo AS skuInfoList,
    COALESCE(daily_snapshots.firstCollectDate, daily_snapshots.stat_date) AS firstAddTime,
    daily_snapshots.`date` AS lastUpdateTime
FROM daily_snapshots;
