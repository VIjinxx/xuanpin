-- DWS full rebuild insert.
-- This script only inserts rebuilt historical rows. Clear the target DWS table outside this script when needed.

INSERT INTO dws_category_daily
(
    optId,
    siteId,
    date,
    parentId,
    optNameEnglish,
    optNameChinese,
    imageUrl,
    seoLinkUrl,
    linkUrl,
    shareUrl,
    optType,
    goodsNum,
    isFeatured,
    rank,
    totalSalesVolume,
    dailySalesVolume,
    totalSalesAmount,
    totalGoodsNum,
    dailyGoodsNum,
    halfPipelineGoodsNum,
    totalStoreNum,
    dailyStoreNum,
    avgCustomerPrice,
    dailySalesVolumeChange
)
WITH category_dates AS (
    SELECT DISTINCT `date` AS stat_date
    FROM dwd_category_base
    WHERE `date` IS NOT NULL

    UNION

    SELECT DISTINCT `date` AS stat_date
    FROM dwd_goods_base
    WHERE `date` IS NOT NULL

    UNION

    SELECT DISTINCT `date` AS stat_date
    FROM dwd_malls_base
    WHERE `date` IS NOT NULL
),
category_base AS (
    SELECT
        ranked_category.optId AS optId,
        ranked_category.siteId AS siteId,
        ranked_category.stat_date AS date,
        ranked_category.parentId AS parentId,
        ranked_category.optName AS optNameEnglish,
        ranked_category.optName AS optNameChinese,
        ranked_category.imageUrl AS imageUrl,
        ranked_category.seoLinkUrl AS seoLinkUrl,
        ranked_category.linkUrl AS linkUrl,
        ranked_category.shareUrl AS shareUrl,
        ranked_category.optType AS optType,
        COALESCE(ranked_category.isFeatured, false) AS isFeatured,
        ranked_category.priority AS rank
    FROM (
        SELECT
            dwd.*,
            dates.stat_date,
            ROW_NUMBER() OVER (
                PARTITION BY dwd.optId, dwd.siteId, dates.stat_date
                ORDER BY dwd.date DESC
            ) AS row_num
        FROM dwd_category_base dwd
        JOIN category_dates dates
            ON dwd.date <= dates.stat_date
        WHERE dwd.optId IS NOT NULL
            AND dwd.siteId IS NOT NULL
    ) ranked_category
    WHERE ranked_category.row_num = 1
),
goods_path_records_raw AS (
    SELECT
        dates.stat_date,
        goods.goodsId,
        goods.siteId,
        goods.date,
        CAST(JSON_EXTRACT(goods.crumbOptList, '$[0].optId') AS INT) AS optId,
        goods.salesNum,
        goods.minOnSalePrice,
        goods.semiManaged
    FROM dwd_goods_base goods
    JOIN category_dates dates
        ON goods.date <= dates.stat_date
    WHERE JSON_EXTRACT(goods.crumbOptList, '$[0].optId') IS NOT NULL

    UNION ALL

    SELECT
        dates.stat_date,
        goods.goodsId,
        goods.siteId,
        goods.date,
        CAST(JSON_EXTRACT(goods.crumbOptList, '$[1].optId') AS INT) AS optId,
        goods.salesNum,
        goods.minOnSalePrice,
        goods.semiManaged
    FROM dwd_goods_base goods
    JOIN category_dates dates
        ON goods.date <= dates.stat_date
    WHERE JSON_EXTRACT(goods.crumbOptList, '$[1].optId') IS NOT NULL

    UNION ALL

    SELECT
        dates.stat_date,
        goods.goodsId,
        goods.siteId,
        goods.date,
        CAST(JSON_EXTRACT(goods.crumbOptList, '$[2].optId') AS INT) AS optId,
        goods.salesNum,
        goods.minOnSalePrice,
        goods.semiManaged
    FROM dwd_goods_base goods
    JOIN category_dates dates
        ON goods.date <= dates.stat_date
    WHERE JSON_EXTRACT(goods.crumbOptList, '$[2].optId') IS NOT NULL

    UNION ALL

    SELECT
        dates.stat_date,
        goods.goodsId,
        goods.siteId,
        goods.date,
        CAST(goods.optId AS INT) AS optId,
        goods.salesNum,
        goods.minOnSalePrice,
        goods.semiManaged
    FROM dwd_goods_base goods
    JOIN category_dates dates
        ON goods.date <= dates.stat_date
    WHERE goods.optId IS NOT NULL
        AND JSON_EXTRACT(goods.crumbOptList, '$[0].optId') IS NULL
        AND JSON_EXTRACT(goods.crumbOptList, '$[1].optId') IS NULL
        AND JSON_EXTRACT(goods.crumbOptList, '$[2].optId') IS NULL
),
goods_path_records AS (
    SELECT
        stat_date,
        goodsId,
        siteId,
        date,
        optId,
        salesNum,
        minOnSalePrice,
        semiManaged
    FROM goods_path_records_raw
    WHERE optId IS NOT NULL
    GROUP BY stat_date, goodsId, siteId, date, optId, salesNum, minOnSalePrice, semiManaged
),
site_goods_metrics AS (
    SELECT
        goods.stat_date,
        goods.siteId,
        COUNT(DISTINCT goods.goodsId) AS totalGoodsNum
    FROM goods_path_records goods
    GROUP BY goods.stat_date, goods.siteId
),
category_goods_metrics AS (
    SELECT
        goods.stat_date,
        goods.optId,
        goods.siteId,
        COUNT(DISTINCT goods.goodsId) AS goodsNum,
        COUNT(DISTINCT CASE
            WHEN goods.date = goods.stat_date
            THEN goods.goodsId
        END) AS dailyGoodsNum,
        COUNT(DISTINCT CASE
            WHEN goods.semiManaged = true
            THEN goods.goodsId
        END) AS halfPipelineGoodsNum,
        SUM(CASE
            WHEN goods.date = goods.stat_date
            THEN COALESCE(goods.salesNum, 0)
            ELSE 0
        END) AS dailySalesVolume
    FROM goods_path_records goods
    GROUP BY goods.stat_date, goods.optId, goods.siteId
),
previous_daily_sales_metrics AS (
    SELECT
        goods.stat_date,
        goods.optId,
        goods.siteId,
        SUM(CASE
            WHEN goods.date = DATE_SUB(goods.stat_date, INTERVAL 1 DAY)
            THEN COALESCE(goods.salesNum, 0)
            ELSE 0
        END) AS previousDailySalesVolume
    FROM goods_path_records goods
    WHERE goods.date = DATE_SUB(goods.stat_date, INTERVAL 1 DAY)
    GROUP BY goods.stat_date, goods.optId, goods.siteId
),
latest_category_goods AS (
    SELECT
        ranked_goods.stat_date,
        ranked_goods.optId,
        ranked_goods.siteId,
        ranked_goods.goodsId,
        ranked_goods.salesNum,
        ranked_goods.minOnSalePrice
    FROM (
        SELECT
            goods.stat_date,
            goods.optId,
            goods.siteId,
            goods.goodsId,
            goods.salesNum,
            goods.minOnSalePrice,
            ROW_NUMBER() OVER (
                PARTITION BY goods.stat_date, goods.goodsId, goods.siteId, goods.optId
                ORDER BY goods.date DESC
            ) AS row_num
        FROM goods_path_records goods
    ) ranked_goods
    WHERE ranked_goods.row_num = 1
),
category_sales_metrics AS (
    SELECT
        goods.stat_date,
        goods.optId,
        goods.siteId,
        SUM(COALESCE(goods.salesNum, 0)) AS totalSalesVolume,
        ROUND(
            SUM(COALESCE(goods.salesNum, 0) * COALESCE(goods.minOnSalePrice, 0)) / 100.0,
            2
        ) AS totalSalesAmount
    FROM latest_category_goods goods
    GROUP BY goods.stat_date, goods.optId, goods.siteId
),
goods_metrics AS (
    SELECT
        category_goods_metrics.stat_date,
        category_goods_metrics.optId,
        category_goods_metrics.siteId,
        category_goods_metrics.goodsNum,
        category_goods_metrics.dailyGoodsNum,
        category_goods_metrics.halfPipelineGoodsNum,
        COALESCE(category_sales_metrics.totalSalesVolume, 0) AS totalSalesVolume,
        COALESCE(category_goods_metrics.dailySalesVolume, 0) AS dailySalesVolume,
        COALESCE(previous_daily_sales_metrics.previousDailySalesVolume, 0) AS previousDailySalesVolume,
        COALESCE(category_sales_metrics.totalSalesAmount, 0) AS totalSalesAmount
    FROM category_goods_metrics
    LEFT JOIN category_sales_metrics
        ON category_goods_metrics.stat_date = category_sales_metrics.stat_date
        AND category_goods_metrics.optId = category_sales_metrics.optId
        AND category_goods_metrics.siteId = category_sales_metrics.siteId
    LEFT JOIN previous_daily_sales_metrics
        ON category_goods_metrics.stat_date = previous_daily_sales_metrics.stat_date
        AND category_goods_metrics.optId = previous_daily_sales_metrics.optId
        AND category_goods_metrics.siteId = previous_daily_sales_metrics.siteId
),
latest_store_dates AS (
    SELECT
        dates.stat_date,
        malls.siteId,
        MAX(malls.date) AS latestStoreDate
    FROM dwd_malls_base malls
    JOIN category_dates dates
        ON malls.date <= dates.stat_date
    GROUP BY dates.stat_date, malls.siteId
),
store_metrics AS (
    SELECT
        category_base.date,
        category_base.optId,
        category_base.siteId,
        COUNT(DISTINCT malls.mallId) AS totalStoreNum,
        COUNT(DISTINCT CASE
            WHEN malls.date = latest_store_dates.latestStoreDate
            THEN malls.mallId
        END) AS dailyStoreNum
    FROM category_base
    LEFT JOIN latest_store_dates
        ON latest_store_dates.stat_date = category_base.date
        AND latest_store_dates.siteId = category_base.siteId
    LEFT JOIN dwd_malls_base malls
        ON malls.date <= category_base.date
        AND malls.siteId = category_base.siteId
        AND JSON_CONTAINS(malls.optList, JSON_OBJECT('optId', category_base.optId))
    GROUP BY category_base.date, category_base.optId, category_base.siteId
)
SELECT
    category_base.optId,
    category_base.siteId,
    category_base.date AS `date`,
    category_base.parentId AS parentId,
    category_base.optNameEnglish AS optNameEnglish,
    category_base.optNameChinese AS optNameChinese,
    category_base.imageUrl AS imageUrl,
    category_base.seoLinkUrl AS seoLinkUrl,
    category_base.linkUrl AS linkUrl,
    category_base.shareUrl AS shareUrl,
    category_base.optType AS optType,
    COALESCE(goods_metrics.goodsNum, 0) AS goodsNum,
    category_base.isFeatured AS isFeatured,
    category_base.rank AS rank,
    COALESCE(goods_metrics.totalSalesVolume, 0) AS totalSalesVolume,
    COALESCE(goods_metrics.dailySalesVolume, 0) AS dailySalesVolume,
    COALESCE(goods_metrics.totalSalesAmount, 0) AS totalSalesAmount,
    COALESCE(site_goods_metrics.totalGoodsNum, 0) AS totalGoodsNum,
    COALESCE(goods_metrics.dailyGoodsNum, 0) AS dailyGoodsNum,
    COALESCE(goods_metrics.halfPipelineGoodsNum, 0) AS halfPipelineGoodsNum,
    COALESCE(store_metrics.totalStoreNum, 0) AS totalStoreNum,
    COALESCE(store_metrics.dailyStoreNum, 0) AS dailyStoreNum,
    CASE
        WHEN COALESCE(goods_metrics.totalSalesVolume, 0) = 0
        THEN 0
        ELSE ROUND(goods_metrics.totalSalesAmount / goods_metrics.totalSalesVolume, 2)
    END AS avgCustomerPrice,
    CASE
        WHEN COALESCE(goods_metrics.previousDailySalesVolume, 0) = 0
        THEN CAST(NULL AS DECIMAL(18,4))
        ELSE ROUND(
            (goods_metrics.dailySalesVolume - goods_metrics.previousDailySalesVolume)
            / goods_metrics.previousDailySalesVolume,
            4
        )
    END AS dailySalesVolumeChange
FROM category_base
LEFT JOIN goods_metrics
    ON category_base.date = goods_metrics.stat_date
    AND category_base.optId = goods_metrics.optId
    AND category_base.siteId = goods_metrics.siteId
LEFT JOIN site_goods_metrics
    ON category_base.date = site_goods_metrics.stat_date
    AND category_base.siteId = site_goods_metrics.siteId
LEFT JOIN store_metrics
    ON category_base.date = store_metrics.date
    AND category_base.optId = store_metrics.optId
    AND category_base.siteId = store_metrics.siteId;
