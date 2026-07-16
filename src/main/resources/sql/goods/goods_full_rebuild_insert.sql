-- DWS full rebuild insert.
-- This script only inserts rebuilt historical rows. Clear the target DWS table outside this script when needed.

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
WITH goods_dates AS (
    SELECT DISTINCT `date` AS stat_date
    FROM dwd_goods_base
    WHERE `date` IS NOT NULL
),
latest_goods AS (
    SELECT
        ranked_goods.*
    FROM (
        SELECT
            dwd.*,
            dates.stat_date,
            ROW_NUMBER() OVER (
                PARTITION BY dwd.goodsId, dwd.siteId, dates.stat_date
                ORDER BY dwd.`date` DESC
            ) AS row_num
        FROM dwd_goods_base dwd
        JOIN goods_dates dates
            ON dwd.`date` <= dates.stat_date
        WHERE dwd.goodsId IS NOT NULL
            AND dwd.siteId IS NOT NULL
    ) ranked_goods
    WHERE ranked_goods.row_num = 1
),
gallery_urls AS (
    SELECT
        goodsId,
        siteId,
        `date`,
        CAST(CONCAT('[', GROUP_CONCAT(JSON_QUOTE(url) ORDER BY priority), ']') AS JSON) AS carouselImageUrls
    FROM (
        SELECT
            goodsId,
            siteId,
            `date`,
            url,
            MIN(priority) AS priority
        FROM (
            SELECT
                dwd_gallery.goodsId,
                dwd_gallery.siteId,
                dwd_gallery.`date`,
                JSON_EXTRACT_STRING(gallery_item, '$.url') AS url,
                CAST(JSON_EXTRACT_STRING(gallery_item, '$.priority') AS INT) AS priority
            FROM dwd_goods_base dwd_gallery
            LATERAL VIEW explode_json_array_json(dwd_gallery.gallery) exploded_gallery AS gallery_item

            UNION ALL

            SELECT
                dwd_gallery.goodsId,
                dwd_gallery.siteId,
                dwd_gallery.`date`,
                JSON_EXTRACT_STRING(dwd_gallery.gallery, '$.url') AS url,
                CAST(JSON_EXTRACT_STRING(dwd_gallery.gallery, '$.priority') AS INT) AS priority
            FROM dwd_goods_base dwd_gallery
        ) raw_gallery_urls
        WHERE NULLIF(url, '') IS NOT NULL
        GROUP BY goodsId, siteId, `date`, url
    ) distinct_gallery_urls
    GROUP BY goodsId, siteId, `date`
),
sku_stock AS (
    SELECT
        sku_goods.goodsId,
        sku_goods.siteId,
        sku_goods.`date`,
        SUM(COALESCE(JSON_EXTRACT_BIGINT(sku_goods.skuList, CONCAT('$."', sku_key, '".stockQuantity')), 0)) AS stockQuantity,
        MAX(COALESCE(JSON_EXTRACT_BIGINT(sku_goods.skuList, CONCAT('$."', sku_key, '".isOnsale')), 0)) AS maxSkuIsOnsale
    FROM dwd_goods_base sku_goods
    LATERAL VIEW explode(JSON_KEYS(sku_goods.skuList)) exploded_keys AS sku_key
    WHERE sku_goods.skuList IS NOT NULL
        AND JSON_LENGTH(sku_goods.skuList) > 0
    GROUP BY sku_goods.goodsId, sku_goods.siteId, sku_goods.`date`
),
first_collect_dates AS (
    SELECT
        dwd.goodsId,
        dwd.siteId,
        dates.stat_date,
        MIN(dwd.`date`) AS firstCollectDate
    FROM dwd_goods_base dwd
    JOIN goods_dates dates
        ON dwd.`date` <= dates.stat_date
    WHERE dwd.goodsId IS NOT NULL
        AND dwd.siteId IS NOT NULL
    GROUP BY dwd.goodsId, dwd.siteId, dates.stat_date
),
first_review_dates AS (
    SELECT
        review.goodsId,
        review.siteId,
        dates.stat_date,
        MIN(CAST(FROM_UNIXTIME(review.`time`) AS DATE)) AS firstReviewDate
    FROM dwd_review_base review
    JOIN goods_dates dates
        ON CAST(FROM_UNIXTIME(review.`time`) AS DATE) <= dates.stat_date
    WHERE review.goodsId IS NOT NULL
        AND review.siteId IS NOT NULL
        AND review.`time` IS NOT NULL
    GROUP BY review.goodsId, review.siteId, dates.stat_date
),
first_goods_dates AS (
    SELECT
        first_collect.goodsId,
        first_collect.siteId,
        first_collect.stat_date,
        first_collect.firstCollectDate,
        CASE
            WHEN first_review.firstReviewDate IS NULL THEN first_collect.firstCollectDate
            WHEN first_review.firstReviewDate <= first_collect.firstCollectDate THEN first_review.firstReviewDate
            ELSE first_collect.firstCollectDate
        END AS addToRegionTime
    FROM first_collect_dates first_collect
    LEFT JOIN first_review_dates first_review
        ON CAST(first_collect.goodsId AS VARCHAR) = first_review.goodsId
        AND first_collect.siteId = first_review.siteId
        AND first_collect.stat_date = first_review.stat_date
)
SELECT
    latest_goods.goodsId,
    latest_goods.siteId,
    latest_goods.stat_date AS `date`,
    latest_goods.title AS goodsTitleEnglish,
    latest_goods.title AS goodsTitleChinese,
    latest_goods.mallId AS mallId,
    COALESCE(
        NULLIF(latest_goods.imageUrl, ''),
        NULLIF(latest_goods.thumbUrl, ''),
        NULLIF(JSON_EXTRACT_STRING(latest_goods.image, '$.url'), '')
    ) AS mainImageUrl,
    latest_goods.thumbUrl AS thumbUrl,
    COALESCE(
        gallery_urls.carouselImageUrls,
        CASE
            WHEN COALESCE(
                NULLIF(latest_goods.imageUrl, ''),
                NULLIF(latest_goods.thumbUrl, ''),
                NULLIF(JSON_EXTRACT_STRING(latest_goods.image, '$.url'), '')
            ) IS NOT NULL THEN JSON_ARRAY(COALESCE(
                NULLIF(latest_goods.imageUrl, ''),
                NULLIF(latest_goods.thumbUrl, ''),
                NULLIF(JSON_EXTRACT_STRING(latest_goods.image, '$.url'), '')
            ))
            ELSE CAST('[]' AS JSON)
        END
    ) AS carouselImageUrls,
    latest_goods.optId AS optId,
    latest_goods.catId AS catId,
    latest_goods.adultGoods AS isAdultGoods,
    latest_goods.video AS videoInfo,
    latest_goods.goodsProperty AS propertyList,
    latest_goods.salesNum AS salesNum,
    latest_goods.selectedCurrency AS currency,
    latest_goods.minOnSalePrice AS minOnSalePrice,
    latest_goods.maxOnSalePrice AS maxOnSalePrice,
    latest_goods.goodsScore AS score,
    latest_goods.reviewNum AS reviewNum,
    CASE
        WHEN COALESCE(latest_goods.isOnsale, sku_stock.maxSkuIsOnsale, 0) = 1
            AND COALESCE(sku_stock.stockQuantity, 0) > 0 THEN 0
        ELSE 2
    END AS stockStatus,
    COALESCE(sku_stock.stockQuantity, 0) AS stockQuantity,
    COALESCE(first_goods_dates.addToRegionTime, latest_goods.stat_date) AS addToRegionTime,
    latest_goods.skuList AS skuInfoList,
    COALESCE(first_goods_dates.firstCollectDate, latest_goods.stat_date) AS firstAddTime,
    latest_goods.`date` AS lastUpdateTime
FROM latest_goods
LEFT JOIN gallery_urls
    ON latest_goods.goodsId = gallery_urls.goodsId
    AND latest_goods.siteId = gallery_urls.siteId
    AND latest_goods.`date` = gallery_urls.`date`
LEFT JOIN sku_stock
    ON latest_goods.goodsId = sku_stock.goodsId
    AND latest_goods.siteId = sku_stock.siteId
    AND latest_goods.`date` = sku_stock.`date`
LEFT JOIN first_goods_dates
    ON latest_goods.goodsId = first_goods_dates.goodsId
    AND latest_goods.siteId = first_goods_dates.siteId
    AND latest_goods.stat_date = first_goods_dates.stat_date;

