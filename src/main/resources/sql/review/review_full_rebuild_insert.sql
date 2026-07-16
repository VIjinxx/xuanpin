-- DWS full rebuild insert.
-- This script only inserts rebuilt historical rows. Clear the target DWS table outside this script when needed.

INSERT INTO dws_review_daily
(
    `date`,
    reviewId,
    commentEnglish,
    commentChinese,
    `time`,
    `name`,
    avatar,
    profileLinkUrl,
    score,
    goodsId,
    skuId,
    imageOrVideoList,
    specs
)
WITH review_dates AS (
    SELECT DISTINCT `date` AS stat_date
    FROM dwd_review_base
    WHERE `date` IS NOT NULL
)
SELECT
    dwd.`date` AS `date`,
    dwd.reviewId,
    dwd.`comment` AS commentEnglish,
    CASE
        WHEN dwd.lang IN ('zh-Hans', 'zh-CN', 'zh') THEN dwd.`comment`
        ELSE COALESCE(
                JSON_EXTRACT_STRING(dwd.reviewLang, '$.translateComment'),
                dwd.`comment`
             )
        END AS commentChinese,
    dwd.`time`,
    dwd.`name`,
    dwd.avatar,
    dwd.profileLinkUrl,
    dwd.score,
    dwd.goodsId,
    dwd.skuId,
    CAST(
            COALESCE(
                    CAST(dwd.`list` AS STRING),
                    CAST(dwd.pictures AS STRING)
            ) AS JSON
    ) AS imageOrVideoList,
    CAST(
            CAST(dwd.specs AS STRING) AS JSON
    ) AS specs
FROM dwd_review_base dwd
JOIN review_dates dates
    ON dwd.`date` = dates.stat_date;

