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
WITH latest_mall AS (
    SELECT
        mallId,
        siteId,
        mallName,
        mallLogo,
        semiManaged,
        optList,
        goodsNum,
        goodsSalesNum,
        mallStar,
        scoreNumInfoList,
        reviewNum,
        followerNum,
        lastUpdateTime
    FROM (
        SELECT
            mallId,
            siteId,
            `date`,
            mallName,
            mallLogo,
            semiManaged,
            optList,
            goodsNum,
            goodsSalesNum,
            mallStar,
            scoreNumInfoList,
            reviewNum,
            followerNum,
            lastUpdateTime,
            ROW_NUMBER() OVER (
                PARTITION BY mallId, siteId
                ORDER BY `date` DESC
            ) AS rn
        FROM dwd_malls_base
        WHERE `date` = '$[yyyy-MM-dd-1]'
            AND mallId IS NOT NULL
            AND siteId IS NOT NULL
    ) ranked
    WHERE ranked.rn = 1
),
store_goods_sales AS (
    SELECT
        latest_goods.mallId,
        latest_goods.siteId,
        SUM(
            COALESCE(latest_goods.salesNum, 0)
            * COALESCE(latest_goods.minOnSalePrice, latest_goods.maxOnSalePrice, 0)
        ) AS goodsSalesPrice
    FROM (
        SELECT
            goods.mallId,
            goods.siteId,
            goods.goodsId,
            goods.salesNum,
            goods.minOnSalePrice,
            goods.maxOnSalePrice,
            ROW_NUMBER() OVER (
                PARTITION BY goods.mallId, goods.siteId, goods.goodsId
                ORDER BY goods.`date` DESC
            ) AS rn
        FROM dwd_goods_base goods
        WHERE goods.`date` = '$[yyyy-MM-dd-1]'
            AND goods.mallId IS NOT NULL
            AND goods.siteId IS NOT NULL
            AND goods.goodsId IS NOT NULL
    ) latest_goods
    WHERE latest_goods.rn = 1
    GROUP BY latest_goods.mallId, latest_goods.siteId
),
first_mall_dates AS (
    SELECT
        mallId,
        siteId,
        MIN(`date`) AS firstCollectDate
    FROM dwd_malls_base
    WHERE mallId IS NOT NULL
        AND siteId IS NOT NULL
        AND `date` IS NOT NULL
    GROUP BY mallId, siteId
),
existing_dws AS (
    SELECT
        mallId,
        siteId,
        MIN(addToRegionTime) AS addToRegionTime
    FROM dws_malls_daily
    GROUP BY mallId, siteId
)
SELECT
    latest_mall.mallId,
    latest_mall.siteId,
    CAST('$[yyyy-MM-dd-1]' AS DATE) AS `date`,
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
    CASE
        WHEN existing_dws.addToRegionTime IS NULL THEN COALESCE(first_mall_dates.firstCollectDate, CAST('$[yyyy-MM-dd-1]' AS DATE))
        WHEN first_mall_dates.firstCollectDate IS NULL THEN existing_dws.addToRegionTime
        WHEN existing_dws.addToRegionTime <= first_mall_dates.firstCollectDate THEN existing_dws.addToRegionTime
        ELSE first_mall_dates.firstCollectDate
    END AS addToRegionTime,
    latest_mall.lastUpdateTime AS lastUpdateTime
FROM latest_mall
LEFT JOIN store_goods_sales
    ON latest_mall.mallId = store_goods_sales.mallId
    AND latest_mall.siteId = store_goods_sales.siteId
LEFT JOIN existing_dws
    ON latest_mall.mallId = existing_dws.mallId
    AND latest_mall.siteId = existing_dws.siteId
LEFT JOIN first_mall_dates
    ON latest_mall.mallId = first_mall_dates.mallId
    AND latest_mall.siteId = first_mall_dates.siteId;
