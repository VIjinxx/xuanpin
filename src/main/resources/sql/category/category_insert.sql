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
WITH biz_day AS (
    SELECT CAST('$[yyyy-MM-dd-1]' AS DATE) AS stat_date
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
            biz_day.stat_date,
            ROW_NUMBER() OVER (
                PARTITION BY dwd.optId, dwd.siteId
                ORDER BY dwd.date DESC
            ) AS row_num
        FROM dwd_category_base dwd
        CROSS JOIN biz_day
        WHERE dwd.date = biz_day.stat_date
            AND dwd.optId IS NOT NULL
            AND dwd.siteId IS NOT NULL
    ) ranked_category
    WHERE ranked_category.row_num = 1
),
goods_base_yesterday AS (
    SELECT *
    FROM dwd_goods_base goods
    CROSS JOIN biz_day
    WHERE goods.date = biz_day.stat_date
),
goods_path_records_raw AS (
    SELECT
        goods.stat_date,
        goods.goodsId,
        goods.siteId,
        goods.date,
        CAST(JSON_EXTRACT(goods.crumbOptList, '$[0].optId') AS INT) AS optId,
        goods.salesNum,
        goods.minOnSalePrice,
        goods.semiManaged
    FROM goods_base_yesterday goods
    WHERE JSON_EXTRACT(goods.crumbOptList, '$[0].optId') IS NOT NULL

    UNION ALL

    SELECT
        goods.stat_date,
        goods.goodsId,
        goods.siteId,
        goods.date,
        CAST(JSON_EXTRACT(goods.crumbOptList, '$[1].optId') AS INT) AS optId,
        goods.salesNum,
        goods.minOnSalePrice,
        goods.semiManaged
    FROM goods_base_yesterday goods
    WHERE JSON_EXTRACT(goods.crumbOptList, '$[1].optId') IS NOT NULL

    UNION ALL

    SELECT
        goods.stat_date,
        goods.goodsId,
        goods.siteId,
        goods.date,
        CAST(JSON_EXTRACT(goods.crumbOptList, '$[2].optId') AS INT) AS optId,
        goods.salesNum,
        goods.minOnSalePrice,
        goods.semiManaged
    FROM goods_base_yesterday goods
    WHERE JSON_EXTRACT(goods.crumbOptList, '$[2].optId') IS NOT NULL

    UNION ALL

    SELECT
        goods.stat_date,
        goods.goodsId,
        goods.siteId,
        goods.date,
        CAST(goods.optId AS INT) AS optId,
        goods.salesNum,
        goods.minOnSalePrice,
        goods.semiManaged
    FROM goods_base_yesterday goods
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
previous_goods_base AS (
    SELECT *
    FROM dwd_goods_base goods
    CROSS JOIN biz_day
    WHERE goods.date = DATE_SUB(biz_day.stat_date, INTERVAL 1 DAY)
),
previous_goods_path_records_raw AS (
    SELECT
        goods.goodsId,
        goods.siteId,
        goods.date,
        CAST(JSON_EXTRACT(goods.crumbOptList, '$[0].optId') AS INT) AS optId,
        goods.salesNum,
        goods.minOnSalePrice,
        goods.semiManaged
    FROM previous_goods_base goods
    WHERE JSON_EXTRACT(goods.crumbOptList, '$[0].optId') IS NOT NULL

    UNION ALL

    SELECT
        goods.goodsId,
        goods.siteId,
        goods.date,
        CAST(JSON_EXTRACT(goods.crumbOptList, '$[1].optId') AS INT) AS optId,
        goods.salesNum,
        goods.minOnSalePrice,
        goods.semiManaged
    FROM previous_goods_base goods
    WHERE JSON_EXTRACT(goods.crumbOptList, '$[1].optId') IS NOT NULL

    UNION ALL

    SELECT
        goods.goodsId,
        goods.siteId,
        goods.date,
        CAST(JSON_EXTRACT(goods.crumbOptList, '$[2].optId') AS INT) AS optId,
        goods.salesNum,
        goods.minOnSalePrice,
        goods.semiManaged
    FROM previous_goods_base goods
    WHERE JSON_EXTRACT(goods.crumbOptList, '$[2].optId') IS NOT NULL

    UNION ALL

    SELECT
        goods.goodsId,
        goods.siteId,
        goods.date,
        CAST(goods.optId AS INT) AS optId,
        goods.salesNum,
        goods.minOnSalePrice,
        goods.semiManaged
    FROM previous_goods_base goods
    WHERE goods.optId IS NOT NULL
        AND JSON_EXTRACT(goods.crumbOptList, '$[0].optId') IS NULL
        AND JSON_EXTRACT(goods.crumbOptList, '$[1].optId') IS NULL
        AND JSON_EXTRACT(goods.crumbOptList, '$[2].optId') IS NULL
),
previous_goods_path_records AS (
    SELECT
        goodsId,
        siteId,
        date,
        optId,
        salesNum,
        minOnSalePrice,
        semiManaged
    FROM previous_goods_path_records_raw
    WHERE optId IS NOT NULL
    GROUP BY goodsId, siteId, date, optId, salesNum, minOnSalePrice, semiManaged
),
site_goods_metrics AS (
    SELECT
        goods.siteId,
        COUNT(DISTINCT goods.goodsId) AS totalGoodsNum
    FROM goods_base_yesterday goods
    WHERE goods.siteId IS NOT NULL
        AND goods.goodsId IS NOT NULL
    GROUP BY goods.siteId
),
category_goods_metrics AS (
    SELECT
        goods.optId,
        goods.siteId,
        COUNT(DISTINCT goods.goodsId) AS goodsNum,
        COUNT(DISTINCT goods.goodsId) AS dailyGoodsNum,
        COUNT(DISTINCT CASE
            WHEN goods.semiManaged = true
            THEN goods.goodsId
        END) AS halfPipelineGoodsNum,
        SUM(COALESCE(goods.salesNum, 0)) AS dailySalesVolume
    FROM goods_path_records goods
    GROUP BY goods.optId, goods.siteId
),
previous_daily_sales_metrics AS (
    SELECT
        goods.optId,
        goods.siteId,
        SUM(COALESCE(goods.salesNum, 0)) AS previousDailySalesVolume
    FROM previous_goods_path_records goods
    GROUP BY goods.optId, goods.siteId
),
latest_category_goods AS (
    SELECT
        goods.optId,
        goods.siteId,
        goods.goodsId,
        goods.salesNum,
        goods.minOnSalePrice
    FROM goods_path_records goods
),
category_sales_metrics AS (
    SELECT
        goods.optId,
        goods.siteId,
        SUM(COALESCE(goods.salesNum, 0)) AS totalSalesVolume,
        ROUND(
            SUM(COALESCE(goods.salesNum, 0) * COALESCE(goods.minOnSalePrice, 0)) / 100.0,
            2
        ) AS totalSalesAmount
    FROM latest_category_goods goods
    GROUP BY goods.optId, goods.siteId
),
goods_metrics AS (
    SELECT
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
        ON category_goods_metrics.optId = category_sales_metrics.optId
        AND category_goods_metrics.siteId = category_sales_metrics.siteId
    LEFT JOIN previous_daily_sales_metrics
        ON category_goods_metrics.optId = previous_daily_sales_metrics.optId
        AND category_goods_metrics.siteId = previous_daily_sales_metrics.siteId
),
store_metrics AS (
    SELECT
        category_base.date,
        category_base.optId,
        category_base.siteId,
        COUNT(DISTINCT malls.mallId) AS totalStoreNum,
        COUNT(DISTINCT malls.mallId) AS dailyStoreNum
    FROM category_base
    LEFT JOIN dwd_malls_base malls
        ON malls.date = category_base.date
        AND malls.siteId = category_base.siteId
        AND JSON_CONTAINS(malls.optList, JSON_OBJECT('optId', category_base.optId))
    GROUP BY category_base.date, category_base.optId, category_base.siteId
)
SELECT
    category_base.optId,
    category_base.siteId,
    category_base.date,
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
    ON category_base.optId = goods_metrics.optId
    AND category_base.siteId = goods_metrics.siteId
LEFT JOIN site_goods_metrics
    ON category_base.siteId = site_goods_metrics.siteId
LEFT JOIN store_metrics
    ON category_base.date = store_metrics.date
    AND category_base.optId = store_metrics.optId
    AND category_base.siteId = store_metrics.siteId;
