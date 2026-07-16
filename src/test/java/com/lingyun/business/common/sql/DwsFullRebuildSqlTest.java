package com.lingyun.business.common.sql;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DwsFullRebuildSqlTest {

    @Test
    public void fullRebuildSqlIsSplitByDwsTableWithoutSingleDayOrDeleteStatements() throws Exception {
        assertFalse(Files.exists(Paths.get("src/main/resources/sql/dws_full_rebuild_insert.sql")));

        assertSingleInsertOnly("sql/goods/goods_full_rebuild_insert.sql", "dws_goods_daily");
        assertSingleInsertOnly("sql/geekbi_goods/geekbi_goods_full_rebuild_insert.sql", "dws_goods_daily");
        assertSingleInsertOnly("sql/malls/mall_full_rebuild_insert.sql", "dws_malls_daily");
        assertSingleInsertOnly("sql/review/review_full_rebuild_insert.sql", "dws_review_daily");
        assertSingleInsertOnly("sql/category/category_full_rebuild_insert.sql", "dws_category_daily");
    }

    @Test
    public void fullRebuildSqlBuildsDailySnapshotsFromAllSourceDates() throws Exception {
        String goodsSql = readResource("sql/goods/goods_full_rebuild_insert.sql");
        String geekbiGoodsSql = readResource("sql/geekbi_goods/geekbi_goods_full_rebuild_insert.sql");
        String mallSql = readResource("sql/malls/mall_full_rebuild_insert.sql");
        String reviewSql = readResource("sql/review/review_full_rebuild_insert.sql");
        String categorySql = readResource("sql/category/category_full_rebuild_insert.sql");

        assertTrue(goodsSql.contains("goods_dates AS ("));
        assertTrue(goodsSql.contains("PARTITION BY dwd.goodsId, dwd.siteId, dates.stat_date"));
        assertTrue(goodsSql.contains("JOIN goods_dates dates"));
        assertTrue(goodsSql.contains("dwd.`date` <= dates.stat_date"));
        assertTrue(goodsSql.contains("mainImageUrl,\n    thumbUrl,\n    carouselImageUrls"));
        assertTrue(goodsSql.contains("latest_goods.thumbUrl AS thumbUrl"));

        assertTrue(geekbiGoodsSql.contains("geekbi_dates AS ("));
        assertTrue(geekbiGoodsSql.contains("geekbi_goods_intervals AS ("));
        assertTrue(geekbiGoodsSql.contains("LEAD(dwd.`date`) OVER ("));
        assertTrue(geekbiGoodsSql.contains("PARTITION BY dwd.goodsId, dwd.siteId"));
        assertTrue(geekbiGoodsSql.contains("JOIN geekbi_dates dates"));
        assertTrue(geekbiGoodsSql.contains("dates.stat_date >= intervals.`date`"));
        assertTrue(geekbiGoodsSql.contains("FROM dwd_geekbi_goods_daily dwd"));
        assertTrue(geekbiGoodsSql.contains("daily_snapshots.sold AS salesNum"));
        assertTrue(geekbiGoodsSql.contains("daily_snapshots.minPrice * COALESCE(daily_snapshots.cents, 100)"));
        assertFalse(geekbiGoodsSql.contains("ROW_NUMBER()"));

        assertTrue(mallSql.contains("mall_dates AS ("));
        assertTrue(mallSql.contains("PARTITION BY mall_history.mallId, mall_history.siteId, dates.stat_date"));
        assertTrue(mallSql.contains("JOIN mall_dates dates"));
        assertTrue(mallSql.contains("mall_history.`date` = dates.stat_date"));
        assertTrue(mallSql.contains("goods.`date` = dates.stat_date"));
        assertFalse(mallSql.contains("mall_history.`date` <= dates.stat_date"));
        assertFalse(mallSql.contains("goods.`date` <= dates.stat_date"));
        assertFalse(mallSql.contains("<= dates.stat_date"));

        assertTrue(reviewSql.contains("review_dates AS ("));
        assertTrue(reviewSql.contains("JOIN review_dates dates"));
        assertTrue(reviewSql.contains("dwd.`date` = dates.stat_date"));

        assertTrue(categorySql.contains("category_dates AS ("));
        assertTrue(categorySql.contains("PARTITION BY dwd.optId, dwd.siteId, dates.stat_date"));
        assertTrue(categorySql.contains("JOIN category_dates dates"));
        assertTrue(countOccurrences(categorySql, "<= dates.stat_date") >= 6);
    }

    private static void assertSingleInsertOnly(String resourcePath, String tableName) throws Exception {
        String sql = readResource(resourcePath);

        assertEquals(1, countOccurrences(sql, "INSERT INTO dws_"));
        assertTrue(sql.contains("INSERT INTO " + tableName));
        assertFalse(sql.contains("$[yyyy-MM-dd-1]"));
        assertFalse(sql.contains("2026-05-28"));
        assertFalse(sql.toUpperCase().contains("DELETE FROM"));
        assertFalse(sql.toUpperCase().contains("TRUNCATE"));
        assertFalse(sql.toUpperCase().contains("DROP TABLE"));
    }

    private static String readResource(String resourcePath) throws Exception {
        InputStream inputStream = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(resourcePath);
        assertNotNull("resource not found: " + resourcePath, inputStream);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append('\n');
            }
            return builder.toString();
        }
    }

    private static int countOccurrences(String text, String search) {
        int count = 0;
        int index = 0;
        while ((index = text.indexOf(search, index)) >= 0) {
            count++;
            index += search.length();
        }
        return count;
    }
}
