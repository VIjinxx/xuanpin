package com.lingyun.business.geekbiGoods.sql;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GeekbiGoodsInsertSqlTest {

    @Test
    public void geekbiGoodsInsertMapsDailyDwdRowsIntoGoodsDws() throws Exception {
        String sql = readResource("sql/geekbi_goods/geekbi_goods_insert.sql");

        assertTrue(sql.contains("INSERT INTO dws_goods_daily"));
        assertEquals(1, countOccurrences(sql, "INSERT INTO"));
        assertTrue(sql.contains("FROM dwd_geekbi_goods_daily dwd"));
        assertTrue(sql.contains("WHERE dwd.`date` = '$[yyyy-MM-dd-1]'"));
        assertTrue(sql.contains("COALESCE(NULLIF(dwd.goodsNameEn, ''), NULLIF(dwd.goodsName, ''), NULLIF(dwd.goodsNameCn, ''))"));
        assertTrue(sql.contains("COALESCE(NULLIF(dwd.goodsNameCn, ''), NULLIF(dwd.goodsName, ''), NULLIF(dwd.goodsNameEn, ''))"));
        assertTrue(sql.contains("COALESCE(NULLIF(dwd.thumbnail, ''), NULLIF(dwd.thumbnailCn, '')) AS mainImageUrl"));
        assertTrue(sql.contains("dwd.sold AS salesNum"));
        assertTrue(sql.contains("dwd.currency AS currency"));
        assertTrue(sql.contains("dwd.minPrice * COALESCE(dwd.cents, 100)"));
        assertTrue(sql.contains("dwd.maxPrice * COALESCE(dwd.cents, 100)"));
        assertTrue(sql.contains("dwd.quantity AS stockQuantity"));
        assertTrue(sql.contains("dwd.skuInfo AS skuInfoList"));
        assertTrue(sql.contains("CAST(dwd.onSaleTime AS DATE)"));
        assertFalse(sql.toUpperCase().contains("DELETE FROM"));
        assertFalse(sql.toUpperCase().contains("TRUNCATE"));
    }

    @Test
    public void geekbiGoodsFullRebuildInsertBuildsDailySnapshotsFromAllSourceDates() throws Exception {
        String sql = readResource("sql/geekbi_goods/geekbi_goods_full_rebuild_insert.sql");

        assertTrue(sql.contains("INSERT INTO dws_goods_daily"));
        assertEquals(1, countOccurrences(sql, "INSERT INTO"));
        assertTrue(sql.contains("geekbi_dates AS ("));
        assertTrue(sql.contains("geekbi_goods_intervals AS ("));
        assertTrue(sql.contains("daily_snapshots AS ("));
        assertTrue(sql.contains("LEAD(dwd.`date`) OVER ("));
        assertTrue(sql.contains("dates.stat_date >= intervals.`date`"));
        assertFalse(sql.contains("ROW_NUMBER()"));
        assertFalse(sql.contains("$[yyyy-MM-dd-1]"));
        assertFalse(sql.contains("existing_dws"));
        assertFalse(sql.toUpperCase().contains("DELETE FROM"));
        assertFalse(sql.toUpperCase().contains("TRUNCATE"));
    }

    @Test
    public void geekbiGoodsInsertPreservesExistingFirstDates() throws Exception {
        String sql = readResource("sql/geekbi_goods/geekbi_goods_insert.sql");

        assertTrue(sql.contains("MIN(`date`) AS firstCollectDate"));
        assertTrue(sql.contains("MIN(firstAddTime) AS firstAddTime"));
        assertTrue(sql.contains("MIN(addToRegionTime) AS addToRegionTime"));
        assertTrue(sql.contains("existing_dws.firstAddTime"));
        assertTrue(sql.contains("existing_dws.addToRegionTime"));
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
