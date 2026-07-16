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
SELECT
    dwd.goodsId,
    dwd.siteId,
    dwd.date,
    dwd.title AS goodsTitleEnglish,
    dwd.title AS goodsTitleChinese,
    dwd.mallId AS mallId,
    COALESCE(
        NULLIF(dwd.thumbUrl, ''),
        NULLIF(dwd.imageUrl, ''),
        NULLIF(JSON_EXTRACT_STRING(dwd.image, '$.url'), '')
    ) AS mainImageUrl,
    dwd.thumbUrl AS thumbUrl,
    COALESCE(
        gallery_urls.carouselImageUrls,
        CASE
            WHEN COALESCE(
                NULLIF(dwd.imageUrl, ''),
                NULLIF(dwd.thumbUrl, ''),
                NULLIF(JSON_EXTRACT_STRING(dwd.image, '$.url'), '')
            ) IS NOT NULL THEN JSON_ARRAY(COALESCE(
                NULLIF(dwd.imageUrl, ''),
                NULLIF(dwd.thumbUrl, ''),
                NULLIF(JSON_EXTRACT_STRING(dwd.image, '$.url'), '')
            ))
            ELSE CAST('[]' AS JSON)
        END
    ) AS carouselImageUrls,
    dwd.optId AS optId,
    dwd.catId AS catId,
    dwd.adultGoods AS isAdultGoods,
    dwd.video AS videoInfo,
    dwd.goodsProperty AS propertyList,
    dwd.salesNum AS salesNum,
    dwd.selectedCurrency AS currency,
    dwd.minOnSalePrice AS minOnSalePrice,
    dwd.maxOnSalePrice AS maxOnSalePrice,
    dwd.goodsScore AS score,
    dwd.reviewNum AS reviewNum,
    CASE
        WHEN COALESCE(dwd.isOnsale, sku_stock.maxSkuIsOnsale, 0) = 1
            AND COALESCE(sku_stock.stockQuantity, 0) > 0 THEN 0
        ELSE 2
    END AS stockStatus,
    COALESCE(sku_stock.stockQuantity, 0) AS stockQuantity,
    CASE
        WHEN existing_dws.addToRegionTime IS NULL THEN COALESCE(first_goods_dates.addToRegionTime, CAST('$[yyyy-MM-dd-1]' AS DATE))
        WHEN first_goods_dates.addToRegionTime IS NULL THEN existing_dws.addToRegionTime
        WHEN existing_dws.addToRegionTime <= first_goods_dates.addToRegionTime THEN existing_dws.addToRegionTime
        ELSE first_goods_dates.addToRegionTime
    END AS addToRegionTime,
    dwd.skuList AS skuInfoList,
    COALESCE(
        existing_dws.firstAddTime,
        first_goods_dates.firstCollectDate,
        dwd.date
    ) AS firstAddTime,
    dwd.date AS lastUpdateTime
FROM dwd_goods_base dwd
LEFT JOIN (
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
            WHERE dwd_gallery.`date` = '$[yyyy-MM-dd-1]'

            UNION ALL

            SELECT
                dwd_gallery.goodsId,
                dwd_gallery.siteId,
                dwd_gallery.`date`,
                JSON_EXTRACT_STRING(dwd_gallery.gallery, '$.url') AS url,
                CAST(JSON_EXTRACT_STRING(dwd_gallery.gallery, '$.priority') AS INT) AS priority
            FROM dwd_goods_base dwd_gallery
            WHERE dwd_gallery.`date` = '$[yyyy-MM-dd-1]'
        ) raw_gallery_urls
        WHERE NULLIF(url, '') IS NOT NULL
        GROUP BY goodsId, siteId, `date`, url
    ) distinct_gallery_urls
    GROUP BY goodsId, siteId, `date`
) gallery_urls
    ON dwd.goodsId = gallery_urls.goodsId
    AND dwd.siteId = gallery_urls.siteId
    AND dwd.`date` = gallery_urls.`date`
LEFT JOIN (
    SELECT
        sku_goods.goodsId,
        sku_goods.siteId,
        sku_goods.`date`,
        SUM(COALESCE(JSON_EXTRACT_BIGINT(sku_goods.skuList, CONCAT('$."', sku_key, '".stockQuantity')), 0)) AS stockQuantity,
        MAX(COALESCE(JSON_EXTRACT_BIGINT(sku_goods.skuList, CONCAT('$."', sku_key, '".isOnsale')), 0)) AS maxSkuIsOnsale
    FROM dwd_goods_base sku_goods
    LATERAL VIEW explode(JSON_KEYS(sku_goods.skuList)) exploded_keys AS sku_key
    WHERE sku_goods.`date` = '$[yyyy-MM-dd-1]'
        AND sku_goods.skuList IS NOT NULL
        AND JSON_LENGTH(sku_goods.skuList) > 0
    GROUP BY sku_goods.goodsId, sku_goods.siteId, sku_goods.`date`
) sku_stock
    ON dwd.goodsId = sku_stock.goodsId
    AND dwd.siteId = sku_stock.siteId
    AND dwd.`date` = sku_stock.`date`
LEFT JOIN (
    SELECT
        first_collect.goodsId,
        first_collect.siteId,
        first_collect.firstCollectDate,
        CASE
            WHEN first_review.firstReviewDate IS NULL THEN first_collect.firstCollectDate
            WHEN first_review.firstReviewDate <= first_collect.firstCollectDate THEN first_review.firstReviewDate
            ELSE first_collect.firstCollectDate
        END AS addToRegionTime
    FROM (
        SELECT
            goodsId,
            siteId,
            MIN(`date`) AS firstCollectDate
        FROM dwd_goods_base
        WHERE goodsId IS NOT NULL
            AND siteId IS NOT NULL
        GROUP BY goodsId, siteId
    ) first_collect
    LEFT JOIN (
        SELECT
            goodsId,
            siteId,
            MIN(CAST(FROM_UNIXTIME(`time`) AS DATE)) AS firstReviewDate
        FROM dwd_review_base
        WHERE goodsId IS NOT NULL
            AND siteId IS NOT NULL
            AND `time` IS NOT NULL
        GROUP BY goodsId, siteId
    ) first_review
        ON CAST(first_collect.goodsId AS VARCHAR) = first_review.goodsId
        AND first_collect.siteId = first_review.siteId
) first_goods_dates
    ON dwd.goodsId = first_goods_dates.goodsId
    AND dwd.siteId = first_goods_dates.siteId
LEFT JOIN (
    SELECT
        goodsId,
        siteId,
        MIN(firstAddTime) AS firstAddTime,
        MIN(addToRegionTime) AS addToRegionTime
    FROM dws_goods_daily
    GROUP BY goodsId, siteId
) existing_dws
    ON dwd.goodsId = existing_dws.goodsId
    AND dwd.siteId = existing_dws.siteId
WHERE dwd.date = '$[yyyy-MM-dd-1]'
    AND dwd.goodsId IS NOT NULL
    AND dwd.siteId IS NOT NULL;
