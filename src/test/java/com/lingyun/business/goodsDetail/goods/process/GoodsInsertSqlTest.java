package com.lingyun.business.goodsDetail.goods.process;

import com.lingyun.business.common.model.goods.DWSGoodsRecord;
import com.lingyun.business.common.model.goods.GoodsSchema;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public class GoodsInsertSqlTest {

    @Test
    public void goodsInsertFallsBackToDwdImageUrlForDwsImages() throws Exception {
        String sql = readResource("sql/goods/goods_insert.sql");

        assertTrue(countOccurrences(sql, "NULLIF(JSON_EXTRACT_STRING(dwd.image, '$.url'), '')") >= 1);
    }

    @Test
    public void goodsInsertBuildsCarouselImageUrlsFromGallery() throws Exception {
        String sql = readResource("sql/goods/goods_insert.sql");

        assertTrue(sql.contains("explode_json_array_json(dwd_gallery.gallery)"));
        assertTrue(sql.contains("GROUP_CONCAT(JSON_QUOTE(url) ORDER BY priority)"));
        assertTrue(sql.contains("gallery_urls.carouselImageUrls"));
    }

    @Test
    public void goodsInsertBuildsStockFieldsFromSkuList() throws Exception {
        String sql = readResource("sql/goods/goods_insert.sql");

        assertTrue(sql.contains("JSON_KEYS(sku_goods.skuList)"));
        assertTrue(sql.contains("CONCAT('$.\"', sku_key, '\".stockQuantity')"));
        assertTrue(sql.contains("COALESCE(sku_stock.stockQuantity, 0) AS stockQuantity"));
        assertTrue(sql.contains("sku_stock.maxSkuIsOnsale"));
    }

    @Test
    public void goodsInsertBuildsAddToRegionTimeFromReviewAndFirstCollectDates() throws Exception {
        String sql = readResource("sql/goods/goods_insert.sql");

        assertTrue(sql.contains("MIN(`date`) AS firstCollectDate"));
        assertTrue(sql.contains("MIN(CAST(FROM_UNIXTIME(`time`) AS DATE)) AS firstReviewDate"));
        assertTrue(sql.contains("first_goods_dates.addToRegionTime"));
        assertTrue(sql.contains("END AS addToRegionTime"));
        assertTrue(sql.contains("AND dwd.goodsId IS NOT NULL"));
        assertTrue(sql.contains("AND dwd.siteId IS NOT NULL"));
    }

    @Test
    public void goodsDwsDefinesAndInsertsThumbUrlFromDwd() throws Exception {
        String ddl = readResource("sql/goods/dws_goods_daily.sql");
        String sql = readResource("sql/goods/goods_insert.sql");

        assertTrue(ddl.contains("`thumbUrl` TEXT NULL COMMENT '略缩图链接'"));
        assertTrue(sql.contains("mainImageUrl,\n    thumbUrl,\n    carouselImageUrls"));
        assertTrue(sql.contains("dwd.thumbUrl AS thumbUrl"));

        assertNotNull(GoodsSchema.dwsField("thumbUrl"));
        assertEquals("STRING", GoodsSchema.dwsField("thumbUrl").getSqlType());

        DWSGoodsRecord record = new DWSGoodsRecord();
        record.setThumbUrl("thumb-url");
        assertEquals("thumb-url", record.getThumbUrl());
    }

    @Test
    public void goodsOdsDefinesRequestedGoodsDetailFields() throws Exception {
        String ddl = readResource("sql/goods/ods_goods_raw.sql");

        assertTrue(ddl.contains("`businessReduction` STRING COMMENT '商家承担优惠金额'"));
        assertTrue(ddl.contains("`sellerType` BIGINT COMMENT '商家类型编码'"));
        assertTrue(ddl.contains("`searchKey` STRING COMMENT '用户原始搜索关键词'"));
        assertTrue(ddl.contains("`sortTypeStyle` BIGINT COMMENT '评价列表排序样式标识'"));
        assertTrue(ddl.contains("`reviewScore` STRING COMMENT '商品综合评价得分'"));
        assertTrue(ddl.contains("`tagCode` BIGINT COMMENT '店铺权益标签'"));
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
