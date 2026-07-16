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
    dwd.`date`,
    COALESCE(NULLIF(dwd.goodsNameEn, ''), NULLIF(dwd.goodsName, ''), NULLIF(dwd.goodsNameCn, ''))
        AS goodsTitleEnglish,
    COALESCE(NULLIF(dwd.goodsNameCn, ''), NULLIF(dwd.goodsName, ''), NULLIF(dwd.goodsNameEn, ''))
        AS goodsTitleChinese,
    dwd.mallId AS mallId,
    COALESCE(NULLIF(dwd.thumbnail, ''), NULLIF(dwd.thumbnailCn, '')) AS mainImageUrl,
    dwd.thumbnail AS thumbUrl,
    CASE
        WHEN COALESCE(NULLIF(dwd.thumbnail, ''), NULLIF(dwd.thumbnailCn, '')) IS NOT NULL
            THEN JSON_ARRAY(COALESCE(NULLIF(dwd.thumbnail, ''), NULLIF(dwd.thumbnailCn, '')))
        ELSE CAST('[]' AS JSON)
    END AS carouselImageUrls,
    dwd.optId AS optId,
    dwd.catId AS catId,
    NULL AS isAdultGoods,
    NULL AS videoInfo,
    NULL AS propertyList,
    dwd.sold AS salesNum,
    dwd.currency AS currency,
    CASE
        WHEN dwd.minPrice IS NULL THEN NULL
        ELSE CAST(ROUND(dwd.minPrice * COALESCE(dwd.cents, 100), 0) AS BIGINT)
    END AS minOnSalePrice,
    CASE
        WHEN dwd.maxPrice IS NULL THEN NULL
        ELSE CAST(ROUND(dwd.maxPrice * COALESCE(dwd.cents, 100), 0) AS BIGINT)
    END AS maxOnSalePrice,
    CAST(dwd.goodsScore AS DOUBLE) AS score,
    dwd.reviewNum AS reviewNum,
    CASE
        WHEN COALESCE(dwd.status, 0) = 1
            AND COALESCE(dwd.quantity, 0) > 0 THEN 0
        ELSE 2
    END AS stockStatus,
    dwd.quantity AS stockQuantity,
    CASE
        WHEN existing_dws.addToRegionTime IS NULL THEN COALESCE(
            CAST(dwd.onSaleTime AS DATE),
            first_source_dates.firstOnSaleDate,
            first_source_dates.firstCollectDate,
            dwd.`date`
        )
        WHEN COALESCE(
            CAST(dwd.onSaleTime AS DATE),
            first_source_dates.firstOnSaleDate,
            first_source_dates.firstCollectDate
        ) IS NULL THEN existing_dws.addToRegionTime
        WHEN existing_dws.addToRegionTime <= COALESCE(
            CAST(dwd.onSaleTime AS DATE),
            first_source_dates.firstOnSaleDate,
            first_source_dates.firstCollectDate
        ) THEN existing_dws.addToRegionTime
        ELSE COALESCE(
            CAST(dwd.onSaleTime AS DATE),
            first_source_dates.firstOnSaleDate,
            first_source_dates.firstCollectDate
        )
    END AS addToRegionTime,
    dwd.skuInfo AS skuInfoList,
    CASE
        WHEN existing_dws.firstAddTime IS NULL THEN COALESCE(
            first_source_dates.firstCollectDate,
            dwd.`date`
        )
        WHEN first_source_dates.firstCollectDate IS NULL THEN existing_dws.firstAddTime
        WHEN existing_dws.firstAddTime <= first_source_dates.firstCollectDate
            THEN existing_dws.firstAddTime
        ELSE first_source_dates.firstCollectDate
    END AS firstAddTime,
    dwd.`date` AS lastUpdateTime
FROM dwd_geekbi_goods_daily dwd
LEFT JOIN (
    SELECT
        goodsId,
        siteId,
        MIN(`date`) AS firstCollectDate,
        MIN(CAST(onSaleTime AS DATE)) AS firstOnSaleDate
    FROM dwd_geekbi_goods_daily
    WHERE goodsId IS NOT NULL
        AND siteId IS NOT NULL
    GROUP BY goodsId, siteId
) first_source_dates
    ON dwd.goodsId = first_source_dates.goodsId
    AND dwd.siteId = first_source_dates.siteId
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
WHERE dwd.`date` = '$[yyyy-MM-dd-1]'
    AND dwd.goodsId IS NOT NULL
    AND dwd.siteId IS NOT NULL;
