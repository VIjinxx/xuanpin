package com.lingyun.business.goodsDetail.mall.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.lingyun.business.common.model.mall.DWDMallsRecord;
import com.lingyun.business.common.model.mall.ODSMallsRecord;
import com.lingyun.business.common.util.JsonUtil;
import com.lingyun.business.goodsDetail.mall.sink.DorisBatchSink;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MallSemiManagedFlowTest {

    @Test
    public void goodsDetailMallSemiManagedFlowsFromOdsToDwdAsBoolean() throws Exception {
        ODSMallsProcessor odsProcessor = new ODSMallsProcessor();
        DWDMallsProcessor dwdProcessor = new DWDMallsProcessor();
        odsProcessor.open(new Configuration());
        dwdProcessor.open(new Configuration());
        try {
            ODSMallsRecord fullManaged = odsProcessor.map("{\"goods\":{\"mallId\":1001,\"isLocalGoods\":0}}");
            assertNotNull(fullManaged);
            assertEquals(Boolean.FALSE, fullManaged.getSemiManaged());

            DWDMallsRecord fullManagedDwd = dwdProcessor.map(fullManaged);
            assertEquals(Boolean.FALSE, fullManagedDwd.getSemiManaged());
            JsonNode fullManagedJson = JsonUtil.parseJson(DorisBatchSink.dwdRecordToJson(fullManagedDwd));
            assertTrue(fullManagedJson.path("semiManaged").isBoolean());
            assertFalse(fullManagedJson.path("semiManaged").asBoolean());

            ODSMallsRecord semiManaged = odsProcessor.map("{\"goods\":{\"mallId\":1002,\"isLocalGoods\":1}}");
            assertNotNull(semiManaged);
            assertEquals(Boolean.TRUE, semiManaged.getSemiManaged());

            DWDMallsRecord semiManagedDwd = dwdProcessor.map(semiManaged);
            assertEquals(Boolean.TRUE, semiManagedDwd.getSemiManaged());
            JsonNode semiManagedJson = JsonUtil.parseJson(DorisBatchSink.dwdRecordToJson(semiManagedDwd));
            assertTrue(semiManagedJson.path("semiManaged").isBoolean());
            assertTrue(semiManagedJson.path("semiManaged").asBoolean());
        } finally {
            dwdProcessor.close();
            odsProcessor.close();
        }
    }

    @Test
    public void mallInsertBuildsMallTypeFromDwdSemiManaged() throws Exception {
        String sql = readResource("sql/malls/mall_insert.sql");

        assertTrue(sql.contains("FROM dwd_malls_base"));
        assertTrue(sql.contains("WHEN latest_mall.semiManaged = true THEN 1"));
        assertTrue(sql.contains("END AS mallType"));
    }

    @Test
    public void mallInsertBuildsAddToRegionTimeFromFirstCollectDate() throws Exception {
        String sql = readResource("sql/malls/mall_insert.sql");

        assertTrue(sql.contains("first_mall_dates.firstCollectDate"));
        assertTrue(sql.contains("MIN(`date`) AS firstCollectDate"));
        assertTrue(sql.contains("FROM dwd_malls_base"));
        assertTrue(sql.contains("first_mall_dates"));
        assertTrue(sql.contains("latest_mall.mallId = first_mall_dates.mallId"));
        assertTrue(sql.contains("latest_mall.siteId = first_mall_dates.siteId"));
    }

    @Test
    public void mallInsertBuildsGoodsSalesPriceFromGoodsSalesAndPrice() throws Exception {
        String sql = readResource("sql/malls/mall_insert.sql");

        assertTrue(sql.contains("COALESCE(store_goods_sales.goodsSalesPrice, 0) AS goodsSalesPrice"));
        assertTrue(sql.contains("ROW_NUMBER() OVER"));
        assertTrue(sql.contains("latest_goods.rn = 1"));
        assertTrue(sql.contains("SUM("));
        assertTrue(sql.contains("COALESCE(latest_goods.salesNum, 0)"));
        assertTrue(sql.contains("COALESCE(latest_goods.minOnSalePrice, latest_goods.maxOnSalePrice, 0)"));
        assertTrue(sql.contains("dwd_goods_base goods"));
        assertTrue(sql.contains("goods.`date` = '$[yyyy-MM-dd-1]'"));
        assertFalse(sql.contains("goods.`date` <= '$[yyyy-MM-dd-1]'"));
        assertTrue(sql.contains("GROUP BY latest_goods.mallId, latest_goods.siteId"));
        assertTrue(sql.contains("latest_mall.mallId = store_goods_sales.mallId"));
        assertTrue(sql.contains("latest_mall.siteId = store_goods_sales.siteId"));
    }

    @Test
    public void mallInsertUsesYesterdayOnlySnapshotForMallRows() throws Exception {
        String sql = readResource("sql/malls/mall_insert.sql");

        assertTrue(sql.contains("FROM dwd_malls_base"));
        assertTrue(sql.contains("WHERE `date` = '$[yyyy-MM-dd-1]'"));
        assertFalse(sql.contains("WHERE `date` <= '$[yyyy-MM-dd-1]'"));
    }

    @Test
    public void mallOdsDefinesRequestedStoreDetailFields() throws Exception {
        String ddl = readResource("sql/malls/ods_malls_raw.sql");

        assertTrue(ddl.contains("picUrl STRING COMMENT '组件配图 CDN 资源地址'"));
        assertTrue(ddl.contains("avatar STRING COMMENT '商家头像 CDN 在线地址'"));
        assertTrue(ddl.contains("appCode INT COMMENT '入驻渠道编码'"));
        assertTrue(ddl.contains("nickname STRING COMMENT '商家店铺昵称'"));
        assertTrue(ddl.contains("isDefaultAvatar BOOLEAN COMMENT '商家头像'"));
        assertTrue(ddl.contains("reviewNumStr STRING COMMENT '评价数量'"));
        assertTrue(ddl.contains("scoreNumInfoList JSON COMMENT '评价列表'"));
        assertTrue(ddl.contains("marketPriceType INT COMMENT '市场价类型'"));
        assertTrue(ddl.contains("goodsSimpleInfoList STRING COMMENT '单商品数据'"));
        assertTrue(ddl.contains("isAiReview BOOLEAN COMMENT '是否为 AI 虚拟评价'"));
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
}
