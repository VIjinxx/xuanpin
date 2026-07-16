package com.lingyun.business.goodsDetail.category.process;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CategoryInsertSqlTest {

    @Test
    public void categoryInsertUsesBizDateParameterForYesterdayOnlySnapshot() throws Exception {
        String sql = readResource("sql/category/category_insert.sql");

        assertFalse(sql.contains("2026-05-28"));
        assertTrue(sql.contains("biz_day AS ("));
        assertTrue(sql.contains("CAST('$[yyyy-MM-dd-1]' AS DATE) AS stat_date"));
        assertTrue(sql.contains("dwd.date = biz_day.stat_date"));
        assertFalse(sql.contains("dwd.date <= biz_day.stat_date"));
        assertTrue(sql.contains("goods.date = biz_day.stat_date"));
        assertFalse(sql.contains("goods.date <= biz_day.stat_date"));
        assertTrue(sql.contains("PARTITION BY dwd.optId, dwd.siteId"));
        assertTrue(sql.contains("WHERE ranked_category.row_num = 1"));
        assertTrue(countOccurrences(sql, "FROM goods_base_yesterday goods") >= 4);
        assertTrue(sql.contains("previous_goods_base AS ("));
        assertTrue(sql.contains("goods.date = DATE_SUB(biz_day.stat_date, INTERVAL 1 DAY)"));
        assertTrue(countOccurrences(sql, "FROM previous_goods_base goods") >= 4);
        assertTrue(sql.contains("FROM previous_goods_path_records goods"));
        assertTrue(sql.contains("ON malls.date = category_base.date"));
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
