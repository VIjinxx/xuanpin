package com.lingyun.business.goodsDetail.mall.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.lingyun.business.common.model.mall.DWDMallsRecord;
import com.lingyun.business.common.model.mall.ODSMallsRecord;
import com.lingyun.business.common.util.JsonUtil;
import com.lingyun.business.goodsDetail.mall.sink.DorisBatchSink;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MallGoodsSalesNumFallbackTest {

    @Test
    public void goodsDetailMallOdsCleansDorisNumericColumnsBeforeWrite() throws Exception {
        ODSMallsProcessor processor = new ODSMallsProcessor();
        processor.open(new Configuration());
        try {
            ODSMallsRecord record = processor.map("{"
                    + "\"goods\":{\"mallId\":1001,\"isLocalGoods\":1},"
                    + "\"mall\":{\"mallData\":{"
                    + "\"goodsNumUnit\":[\"1,324\",\"Items\"],"
                    + "\"goodsSalesNumUnit\":[\"4.8K\",\"Sold\"],"
                    + "\"followerNumUnit\":[\"70K+\",\"Fans\"],"
                    + "\"mallReviewLabelShow\":true,"
                    + "\"isExpire\":false"
                    + "}},"
                    + "\"review\":{\"reviewData\":{\"mallReview\":{\"mallScore\":\"4.8 points\"}}},"
                    + "\"reviewStore\":{\"mallReview\":{\"reviewNum\":\"2,082\"}},"
                    + "\"webLayoutData\":{\"headerData\":{\"region\":{\"regionInfo\":{\"siteId\":100}}}},"
                    + "\"serverTime\":\"1776391696477\","
                    + "\"lang\":\"zh-Hans\""
                    + "}");

            assertEquals("1324", record.getGoodsNum());
            assertEquals("4800", record.getGoodsSalesNum());
            assertEquals("70000", record.getFollowerNum());
            assertEquals("2082", record.getReviewNum());
            assertEquals("4.8", record.getMallStar());
            assertEquals("1776391696477", record.getServerTime());
            assertOdsMallJsonHasCleanNumericText(DorisBatchSink.odsRecordToJson(record));
        } finally {
            processor.close();
        }
    }

    @Test
    public void storeDetailMallOdsCleansDorisNumericColumnsBeforeWrite() throws Exception {
        com.lingyun.business.storeDetail.mall.process.ODSMallsProcessor processor =
                new com.lingyun.business.storeDetail.mall.process.ODSMallsProcessor();
        processor.open(new Configuration());
        try {
            ODSMallsRecord record = processor.map("{\"page\":{"
                    + "\"mallId\":1002,"
                    + "\"mallInfo\":{"
                    + "\"goodsNumUnit\":[\"1,324\",\"Items\"],"
                    + "\"goodsSalesNum\":\"4.8K\","
                    + "\"followerNum\":\"70K+\","
                    + "\"mallStar\":\"4.8 points\","
                    + "\"reviewNum\":\"2,082\","
                    + "\"serverTime\":\"1776391696477\""
                    + "},"
                    + "\"commentStore\":{\"commentInfo\":{\"mallInfo\":{\"mallReviewLabelShow\":1}}},"
                    + "\"isExpire\":false,"
                    + "\"webLayoutData\":{\"headerData\":{\"region\":{\"regionInfo\":{\"siteId\":100}}}},"
                    + "\"lang\":\"zh-Hans\""
                    + "}}");

            assertEquals("1324", record.getGoodsNum());
            assertEquals("4800", record.getGoodsSalesNum());
            assertEquals("70000", record.getFollowerNum());
            assertEquals("2082", record.getReviewNum());
            assertEquals("4.8", record.getMallStar());
            assertEquals("1776391696477", record.getServerTime());
            assertOdsMallJsonHasCleanNumericText(
                    com.lingyun.business.storeDetail.mall.sink.DorisBatchSink.odsRecordToJson(record));
        } finally {
            processor.close();
        }
    }

    @Test
    public void storeDetailMallMapsRequestedOdsFields() throws Exception {
        com.lingyun.business.storeDetail.mall.process.ODSMallsProcessor processor =
                new com.lingyun.business.storeDetail.mall.process.ODSMallsProcessor();
        processor.open(new Configuration());
        try {
            ODSMallsRecord record = processor.map("{\"page\":{"
                    + "\"mallInfo\":{\"mallInfo\":{\"components\":[{\"picUrl\":\"pic-url\"}]}},"
                    + "\"userInfo\":{\"avatar\":\"avatar-url\",\"appCode\":7,"
                    + "\"nickname\":\"mall-nick\",\"isDefaultAvatar\":true},"
                    + "\"commentStore\":{\"commentInfo\":{\"reviewNumStr\":\"1.2K\","
                    + "\"scoreNumInfoList\":[{\"score\":5,\"num\":12}]}},"
                    + "\"categoryStore\":{\"goodsList\":[{\"data\":{\"priceInfo\":{\"marketPriceType\":2}}}]},"
                    + "\"decorationStore\":{\"decorationData\":{\"Home\":{\"data\":["
                    + "{\"goodsSimpleInfoList\":[{\"goodsId\":11,\"title\":\"item\"}]}"
                    + "]}}},"
                    + "\"commentBigPictureStore\":{\"isAiReview\":false}"
                    + "}}");

            assertNotNull(record);
            assertEquals("pic-url", record.getPicUrl());
            assertEquals("avatar-url", record.getAvatar());
            assertEquals("7", record.getAppCode());
            assertEquals("mall-nick", record.getNickname());
            assertEquals(Boolean.TRUE, record.getIsDefaultAvatar());
            assertEquals("1.2K", record.getReviewNumStr());
            assertEquals("[{\"score\":5,\"num\":12}]", record.getScoreNumInfoList());
            assertEquals("2", record.getMarketPriceType());
            assertEquals("[{\"goodsId\":11,\"title\":\"item\"}]", record.getGoodsSimpleInfoList());
            assertEquals(Boolean.FALSE, record.getIsAiReview());

            JsonNode odsJson = JsonUtil.parseJson(
                    com.lingyun.business.storeDetail.mall.sink.DorisBatchSink.odsRecordToJson(record));
            assertNotNull(odsJson);
            assertEquals("pic-url", odsJson.path("picUrl").asText());
            assertEquals("avatar-url", odsJson.path("avatar").asText());
            assertTrue(odsJson.path("appCode").isIntegralNumber());
            assertEquals(7L, odsJson.path("appCode").asLong());
            assertEquals("mall-nick", odsJson.path("nickname").asText());
            assertTrue(odsJson.path("isDefaultAvatar").isBoolean());
            assertTrue(odsJson.path("isDefaultAvatar").asBoolean());
            assertEquals("1.2K", odsJson.path("reviewNumStr").asText());
            assertEquals("[{\"score\":5,\"num\":12}]", odsJson.path("scoreNumInfoList").asText());
            assertTrue(odsJson.path("marketPriceType").isIntegralNumber());
            assertEquals(2L, odsJson.path("marketPriceType").asLong());
            assertEquals("[{\"goodsId\":11,\"title\":\"item\"}]", odsJson.path("goodsSimpleInfoList").asText());
            assertTrue(odsJson.path("isAiReview").isBoolean());
            assertFalse(odsJson.path("isAiReview").asBoolean());
        } finally {
            processor.close();
        }
    }

    @Test
    public void goodsDetailMallUsesSalesNumUnitWhenSalesNumIsBlank() throws Exception {
        DWDMallsProcessor processor = new DWDMallsProcessor();
        processor.open(new Configuration());
        try {
            DWDMallsRecord record = processor.map(odsRecord(null, "[\"4.8万\",\"已售\"]"));

            assertEquals(Long.valueOf(48000L), record.getGoodsSalesNum());
            assertGoodsSalesNumJsonNumber(record, 48000L);
        } finally {
            processor.close();
        }
    }

    @Test
    public void storeDetailMallUsesSalesNumUnitWhenSalesNumIsBlank() throws Exception {
        com.lingyun.business.storeDetail.mall.process.DWDMallsProcessor processor =
                new com.lingyun.business.storeDetail.mall.process.DWDMallsProcessor();
        processor.open(new Configuration());
        try {
            DWDMallsRecord record = processor.map(odsRecord("", "[\"70K+\",\"Sold\"]"));

            assertEquals(Long.valueOf(70000L), record.getGoodsSalesNum());
            assertGoodsSalesNumJsonNumber(record, 70000L);
        } finally {
            processor.close();
        }
    }

    private static ODSMallsRecord odsRecord(String goodsSalesNum, String goodsSalesNumUnit) {
        ODSMallsRecord record = new ODSMallsRecord();
        record.setMallId("1001");
        record.setSiteId("100");
        record.setDate("2026-05-30");
        record.setGoodsSalesNum(goodsSalesNum);
        record.setGoodsSalesNumUnit(goodsSalesNumUnit);
        return record;
    }

    private static void assertGoodsSalesNumJsonNumber(DWDMallsRecord record, long expected) {
        JsonNode json = JsonUtil.parseJson(DorisBatchSink.dwdRecordToJson(record));
        assertNotNull(json);
        JsonNode goodsSalesNum = json.path("goodsSalesNum");
        assertTrue(goodsSalesNum.isIntegralNumber());
        assertEquals(expected, goodsSalesNum.asLong());
    }

    private static void assertOdsMallJsonHasCleanNumericText(String jsonText) {
        JsonNode json = JsonUtil.parseJson(jsonText);
        assertNotNull(json);
        assertTrue(json.path("siteId").isIntegralNumber());
        assertEquals(100L, json.path("siteId").asLong());
        assertTrue(json.path("goodsNum").isIntegralNumber());
        assertEquals(1324L, json.path("goodsNum").asLong());
        assertTrue(json.path("goodsSalesNum").isIntegralNumber());
        assertEquals(4800L, json.path("goodsSalesNum").asLong());
        assertTrue(json.path("followerNum").isIntegralNumber());
        assertEquals(70000L, json.path("followerNum").asLong());
        assertTrue(json.path("reviewNum").isIntegralNumber());
        assertEquals(2082L, json.path("reviewNum").asLong());
        assertTrue(json.path("mallStar").isNumber());
        assertEquals("4.8", json.path("mallStar").asText());
        assertTrue(json.path("serverTime").isIntegralNumber());
        assertEquals(1776391696477L, json.path("serverTime").asLong());
        assertTrue(json.path("mallReviewLabelShow").isBoolean());
        assertTrue(json.path("mallReviewLabelShow").asBoolean());
        assertTrue(json.path("isExpire").isBoolean());
        assertFalse(json.path("isExpire").asBoolean());
        assertFalse(json.has("lastUpdateTime"));
    }
}
