-- DWS full rebuild insert.
-- This script only inserts rebuilt historical rows. Clear the target DWS table outside this script when needed.

INSERT INTO dws_malls_daily
(
    mallId, siteId, `date`,
    mallName, mallLogoUrl, mallType,
    categoryList,
    goodsNum,
    goodsSalesNum, goodsSalesPrice,
    mallStar, scorePercentInfo,
    reviewNum, followerNum,
    addToRegionTime, lastUpdateTime
)
WITH mall_dates AS (
    SELECT DISTINCT `date` AS stat_date
    FROM dwd_malls_base
    WHERE `date` IS NOT NULL

    UNION

    SELECT DISTINCT `date` AS stat_date
    FROM dwd_goods_base
    WHERE `date` IS NOT NULL
),
latest_mall AS (
    SELECT
        ranked_mall.*
    FROM (
        SELECT
            mall_history.*,
            dates.stat_date,
            ROW_NUMBER() OVER (
                PARTITION BY mall_history.mallId, mall_history.siteId, dates.stat_date
                ORDER BY mall_history.`date` DESC
            ) AS row_num
        FROM dwd_malls_base mall_history
        JOIN mall_dates dates
            ON mall_history.`date` = dates.stat_date
        WHERE mall_history.mallId IS NOT NULL
            AND mall_history.siteId IS NOT NULL
    ) ranked_mall
    WHERE ranked_mall.row_num = 1
),
store_goods_sales AS (
    SELECT
        latest_goods.mallId,
        latest_goods.siteId,
        latest_goods.stat_date,
        SUM(
            COALESCE(latest_goods.salesNum, 0)
            * COALESCE(latest_goods.minOnSalePrice, latest_goods.maxOnSalePrice, 0)
        ) AS goodsSalesPrice
    FROM (
        SELECT
            ranked_goods.*
        FROM (
            SELECT
                goods.mallId,
                goods.siteId,
                goods.goodsId,
                goods.salesNum,
                goods.minOnSalePrice,
                goods.maxOnSalePrice,
                dates.stat_date,
                ROW_NUMBER() OVER (
                    PARTITION BY goods.mallId, goods.siteId, goods.goodsId, dates.stat_date
                    ORDER BY goods.`date` DESC
                ) AS row_num
            FROM dwd_goods_base goods
            JOIN mall_dates dates
                ON goods.`date` = dates.stat_date
            WHERE goods.mallId IS NOT NULL
                AND goods.siteId IS NOT NULL
                AND goods.goodsId IS NOT NULL
        ) ranked_goods
        WHERE ranked_goods.row_num = 1
    ) latest_goods
    GROUP BY latest_goods.mallId, latest_goods.siteId, latest_goods.stat_date
),
first_mall_dates AS (
    SELECT
        mall_history.mallId,
        mall_history.siteId,
        dates.stat_date,
        MIN(mall_history.`date`) AS firstCollectDate
    FROM dwd_malls_base mall_history
    JOIN mall_dates dates
        ON mall_history.`date` = dates.stat_date
    WHERE mall_history.mallId IS NOT NULL
        AND mall_history.siteId IS NOT NULL
        AND mall_history.`date` IS NOT NULL
    GROUP BY mall_history.mallId, mall_history.siteId, dates.stat_date
)
SELECT
    latest_mall.mallId,
    latest_mall.siteId,
    latest_mall.stat_date AS `date`,
    latest_mall.mallName AS mallName,
    latest_mall.mallLogo AS mallLogoUrl,
    CASE
        WHEN latest_mall.semiManaged = true THEN 1
        ELSE 0
    END AS mallType,
    latest_mall.optList AS categoryList,
    latest_mall.goodsNum AS goodsNum,
    latest_mall.goodsSalesNum AS goodsSalesNum,
    COALESCE(store_goods_sales.goodsSalesPrice, 0) AS goodsSalesPrice,
    latest_mall.mallStar AS mallStar,
    latest_mall.scoreNumInfoList AS scorePercentInfo,
    latest_mall.reviewNum AS reviewNum,
    latest_mall.followerNum AS followerNum,
    COALESCE(first_mall_dates.firstCollectDate, latest_mall.stat_date) AS addToRegionTime,
    latest_mall.lastUpdateTime AS lastUpdateTime
FROM latest_mall
LEFT JOIN store_goods_sales
    ON CAST(latest_mall.mallId AS VARCHAR) = store_goods_sales.mallId
    AND latest_mall.siteId = store_goods_sales.siteId
    AND latest_mall.stat_date = store_goods_sales.stat_date
LEFT JOIN first_mall_dates
    ON latest_mall.mallId = first_mall_dates.mallId
    AND latest_mall.siteId = first_mall_dates.siteId
    AND latest_mall.stat_date = first_mall_dates.stat_date;

