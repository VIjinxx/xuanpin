package com.lingyun.business.common.sql;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GeekbiGoodsTableSqlTest {

    @Test
    public void geekbiGoodsOdsKeepsDataFirstLevelAsJsonWithTechnicalKeys() throws Exception {
        String sql = readResource("sql/geekbi_goods/ods_geekbi_goods_raw.sql");

        assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS lingyun.ods_geekbi_goods_raw"));
        assertTrue(sql.contains("`goodsId` BIGINT NOT NULL"));
        assertTrue(sql.contains("`siteId` INT NOT NULL"));
        assertTrue(sql.contains("`date` DATE NOT NULL"));
        assertTrue(sql.contains("`rankings` JSON NULL"));
        assertTrue(sql.contains("`site` JSON NULL"));
        assertTrue(sql.contains("`stats` JSON NULL"));
        assertTrue(sql.contains("`goods` JSON NULL"));
        assertTrue(sql.contains("`history` JSON NULL"));
        assertEquals(5, countOccurrences(sql, " JSON NULL"));
        assertTrue(sql.contains("DUPLICATE KEY(`goodsId`, `siteId`, `date`)"));
        assertTrue(sql.contains("COMMENT '外部数据源商品原始数据表'"));
        assertFalse(sql.contains("third_party"));
    }

    @Test
    public void geekbiGoodsDwdDefinesDailyHistoryMetricsAndGoodsDimensions() throws Exception {
        String sql = readResource("sql/geekbi_goods/dwd_geekbi_goods_daily.sql");

        assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS lingyun.dwd_geekbi_goods_daily"));
        assertTrue(sql.contains("`goodsName` STRING NULL"));
        assertTrue(sql.contains("`mallId` STRING NULL"));
        assertTrue(sql.contains("`currency` STRING NULL"));
        assertTrue(sql.contains("`sold` BIGINT NULL"));
        assertTrue(sql.contains("`sales` DECIMAL(20,4) NULL"));
        assertTrue(sql.contains("`daySold` BIGINT NULL"));
        assertTrue(sql.contains("`daySales` DECIMAL(20,4) NULL"));
        assertTrue(sql.contains("`dayClickNum` BIGINT NULL"));
        assertTrue(sql.contains("`dayExposureNum` BIGINT NULL"));
        assertTrue(sql.contains("`historyCreateTime` DATETIME NULL"));
        assertTrue(sql.contains("UNIQUE KEY(`goodsId`, `siteId`, `date`)"));
        assertTrue(sql.contains("\"enable_unique_key_merge_on_write\" = \"true\""));
        assertTrue(sql.contains("COMMENT '外部数据源商品历史每日明细表'"));
        assertFalse(sql.contains("third_party"));
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
