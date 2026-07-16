package com.lingyun.business.goodsDetail.review.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.lingyun.business.common.model.review.DWDReviewRecord;
import com.lingyun.business.common.model.review.ODSReviewRecord;
import com.lingyun.business.common.util.JsonFileSource;
import com.lingyun.business.common.util.JsonUtil;
import com.lingyun.business.goodsDetail.review.sink.DorisBatchSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ODSReviewProcessorTest {

    @Test
    public void multiSourceMappingsGenerateAlignedReviewRecords() throws Exception {
        String json = "{"
                + "\"review\":{\"reviewData\":{\"reviewInfoList\":["
                + "{\"reviewId\":\"main-1\",\"goodsId\":\"goods-main-1\",\"skuId\":\"sku-main-1\","
                + "\"comment\":\"main comment 1\",\"score\":5,\"time\":101,\"timeMs\":101000,"
                + "\"concatTimeLang\":\"main time 1\",\"concatRichText\":{\"ariaLabel\":\"main rich 1\"},"
                + "\"avatar\":\"avatar-main-1\",\"name\":\"main user 1\",\"isOwnReview\":false,"
                + "\"isSimilarReview\":false,\"profileLinkUrl\":\"/main-1\","
                + "\"specs\":[{\"spec_key\":\"Color\",\"spec_value\":\"Blue\"}],"
                + "\"opList\":[{\"type\":4,\"text\":\"like\"}],\"viewMoreList\":[{\"text\":\"report\"}],"
                + "\"reviewLang\":{\"translateComment\":\"main translated 1\"},"
                + "\"list\":[{\"url\":\"main-image-1\"}],"
                + "\"goodsSpecificReviewLevelInfo\":{\"text\":\"fits\"},"
                + "\"extendParams\":{\"enhanceTranslateText\":\"1\"}},"
                + "{\"reviewId\":\"main-2\",\"goodsId\":\"goods-main-2\",\"skuId\":\"sku-main-2\","
                + "\"comment\":\"main comment 2\",\"score\":4,\"time\":102,\"timeMs\":102000,"
                + "\"concatTimeLang\":\"main time 2\",\"concatRichText\":{\"ariaLabel\":\"main rich 2\"},"
                + "\"avatar\":\"avatar-main-2\",\"name\":\"main user 2\",\"isOwnReview\":false,"
                + "\"isSimilarReview\":false,\"profileLinkUrl\":\"/main-2\"}"
                + "]}},"
                + "\"reviewStore\":{\"commentList\":["
                + "{\"reviewId\":\"store-1\",\"goodsId\":\"goods-store-1\",\"skuId\":\"sku-store-1\","
                + "\"comment\":\"store comment 1\",\"score\":3,\"time\":201,\"timeMs\":201000,"
                + "\"concatTimeLang\":\"store time 1\",\"concatRichText\":{\"ariaLabel\":\"store rich 1\"},"
                + "\"avatar\":\"avatar-store-1\",\"name\":\"store user 1\",\"isOwnReview\":true,"
                + "\"isSimilarReview\":true,\"profileLinkUrl\":\"/store-1\","
                + "\"specs\":[{\"spec_key\":\"Size\",\"spec_value\":\"M\"}],"
                + "\"opList\":[{\"type\":5,\"text\":\"share\"}],\"viewMoreList\":[{\"text\":\"hide\"}],"
                + "\"reviewLang\":{\"translateComment\":\"store translated 1\"},"
                + "\"list\":[{\"url\":\"store-image-1\"}],"
                + "\"goodsSpecificReviewLevelInfo\":{\"text\":\"soft\"},"
                + "\"extendParams\":{\"enhanceTranslateText\":\"0\"}}"
                + "]},"
                + "\"webLayoutData\":{\"headerData\":{\"region\":{\"regionInfo\":{\"siteId\":100,\"selectedLang\":\"en\"}}}},"
                + "\"localInfo\":{\"timezone\":\"Asia/Shanghai\"},"
                + "\"userAgent\":\"test-agent\",\"serverTime\":\"999999\",\"lang\":\"zh-Hans\""
                + "}";

        ODSReviewProcessor processor = new ODSReviewProcessor();
        List<ODSReviewRecord> records = new ArrayList<>();
        processor.open(new Configuration());
        try {
            processor.flatMap(json, new ListCollector(records));
        } finally {
            processor.close();
        }

        assertEquals(3, records.size());

        ODSReviewRecord first = records.get(0);
        assertEquals("main-1", first.getReviewId());
        assertEquals("goods-main-1", first.getGoodsId());
        assertEquals("sku-main-1", first.getSkuId());
        assertEquals("main comment 1", first.getComment());
        assertEquals(Long.valueOf(5), first.getScore());
        assertEquals(Long.valueOf(100), first.getSiteId());
        assertEquals("test-agent", first.getUserAgent());
        assertNotNull(first.getSpecs());
        assertEquals("Blue", first.getSpecs().get(0).path("spec_value").asText());

        ODSReviewRecord second = records.get(1);
        assertEquals("main-2", second.getReviewId());
        assertEquals("goods-main-2", second.getGoodsId());
        assertEquals("main comment 2", second.getComment());

        ODSReviewRecord third = records.get(2);
        assertEquals("store-1", third.getReviewId());
        assertEquals("goods-store-1", third.getGoodsId());
        assertEquals("sku-store-1", third.getSkuId());
        assertEquals("store comment 1", third.getComment());
        assertEquals(Long.valueOf(3), third.getScore());
        assertEquals(Long.valueOf(100), third.getSiteId());
        assertEquals("test-agent", third.getUserAgent());
        assertEquals("Asia/Shanghai", third.getTimezone());
        assertEquals("store rich 1", third.getConcatRichText().path("ariaLabel").asText());
        assertEquals("store-image-1", third.getList().get(0).path("url").asText());
        assertEquals("soft", third.getGoodsSpecificReviewLevelInfo().path("text").asText());
    }

    @Test
    public void missingReviewIdItemsAreDropped() throws Exception {
        String json = "{"
                + "\"review\":{\"reviewData\":{\"reviewInfoList\":["
                + "{\"goodsId\":\"goods-no-id\",\"comment\":\"missing id\"},"
                + "{\"reviewId\":\"   \",\"goodsId\":\"goods-blank-id\",\"comment\":\"blank id\"},"
                + "{\"reviewId\":\"valid-1\",\"goodsId\":\"goods-valid\",\"comment\":\"valid comment\"}"
                + "]}},"
                + "\"webLayoutData\":{\"headerData\":{\"region\":{\"regionInfo\":{\"siteId\":100}}}}"
                + "}";

        ODSReviewProcessor processor = new ODSReviewProcessor();
        List<ODSReviewRecord> records = new ArrayList<>();
        processor.open(new Configuration());
        try {
            processor.flatMap(json, new ListCollector(records));
        } finally {
            processor.close();
        }

        assertEquals(1, records.size());
        assertEquals("valid-1", records.get(0).getReviewId());
        assertEquals(Long.valueOf(100), records.get(0).getSiteId());
        assertNotNull(records.get(0).getDate());
    }

    @Test
    public void optionalReviewFieldsRemainNullInDorisJson() throws Exception {
        String json = "{"
                + "\"review\":{\"reviewData\":{\"reviewInfoList\":["
                + "{\"reviewId\":\"valid-nullable\",\"goodsId\":\"goods-valid\",\"comment\":\"valid comment\"}"
                + "]}},"
                + "\"webLayoutData\":{\"headerData\":{\"region\":{\"regionInfo\":{\"siteId\":100}}}}"
                + "}";

        ODSReviewProcessor processor = new ODSReviewProcessor();
        List<ODSReviewRecord> records = new ArrayList<>();
        processor.open(new Configuration());
        try {
            processor.flatMap(json, new ListCollector(records));
        } finally {
            processor.close();
        }

        ODSReviewRecord odsRecord = records.get(0);
        assertNull(odsRecord.getSkuId());
        assertNull(odsRecord.getSpecs());

        JsonNode odsJson = JsonUtil.parseJson(DorisBatchSink.odsRecordToJson(odsRecord));
        assertNotNull(odsJson);
        assertTrue(odsJson.has("skuId"));
        assertTrue(odsJson.get("skuId").isNull());
        assertTrue(odsJson.has("specs"));
        assertTrue(odsJson.get("specs").isNull());

        DWDReviewRecord dwdRecord = new DWDReviewProcessor().map(odsRecord);
        JsonNode dwdJson = JsonUtil.parseJson(DorisBatchSink.dwdRecordToJson(dwdRecord));
        assertNotNull(dwdJson);
        assertTrue(dwdJson.has("skuId"));
        assertTrue(dwdJson.get("skuId").isNull());
        assertTrue(dwdJson.has("specs"));
        assertTrue(dwdJson.get("specs").isNull());
    }

    @Test
    public void defaultLocalSampleWithoutReviewItemsProducesNoReviewRecords() throws Exception {
        String json = JsonFileSource.readJsonContent("samples/商品详情页.json");

        ODSReviewProcessor processor = new ODSReviewProcessor();
        List<ODSReviewRecord> records = new ArrayList<>();
        processor.open(new Configuration());
        try {
            processor.flatMap(json, new ListCollector(records));
        } finally {
            processor.close();
        }

        assertTrue(records.isEmpty());
    }

    @Test
    public void validLocalReviewSampleProducesRecordWithRequiredKeys() throws Exception {
        String json = JsonFileSource.readJsonContent("samples/review-valid-local.json");

        ODSReviewProcessor processor = new ODSReviewProcessor();
        List<ODSReviewRecord> records = new ArrayList<>();
        processor.open(new Configuration());
        try {
            processor.flatMap(json, new ListCollector(records));
        } finally {
            processor.close();
        }

        assertEquals(1, records.size());
        ODSReviewRecord record = records.get(0);
        assertEquals("codex-review-valid-001", record.getReviewId());
        assertEquals(Long.valueOf(100), record.getSiteId());
        assertNotNull(record.getDate());
        assertTrue(record.getDate().trim().length() > 0);
    }

    @Test
    public void pythonStyleReviewPayloadExpandsMapValueSources() throws Exception {
        String json = "{"
                + "'$ctx': None,"
                + "'needHeadData': True,"
                + "'review': {'reviewData': {'reviewInfoList': ["
                + "{'reviewId': 'main-1', 'goodsId': 'goods-main', 'comment': 'main comment'}"
                + "]}},"
                + "'reviewStore': {"
                + "'commentList': ["
                + "{'reviewId': 'store-1', 'goodsId': 'goods-store', 'comment': 'store comment'}"
                + "],"
                + "'pageReviewListMap': {"
                + "'0': [{'reviewId': 'page-1', 'goodsId': 'goods-page', 'comment': 'page comment'}],"
                + "'1': []"
                + "},"
                + "'initPageReviewListMap': {"
                + "'0': [{'reviewId': 'init-1', 'goodsId': 'goods-init', 'comment': 'init comment'}],"
                + "'1': []"
                + "}"
                + "},"
                + "'webLayoutData': {'headerData': {'region': {'regionInfo': {'siteId': 100}}}},"
                + "'localInfo': {'timezone': 'Asia/Shanghai'},"
                + "'lang': 'en'"
                + "}";

        ODSReviewProcessor processor = new ODSReviewProcessor();
        List<ODSReviewRecord> records = new ArrayList<>();
        processor.open(new Configuration());
        try {
            processor.flatMap(json, new ListCollector(records));
        } finally {
            processor.close();
        }

        assertEquals(4, records.size());
        assertEquals("main-1", records.get(0).getReviewId());
        assertEquals("store-1", records.get(1).getReviewId());
        assertEquals("page-1", records.get(2).getReviewId());
        assertEquals("init-1", records.get(3).getReviewId());
        assertEquals(Long.valueOf(100), records.get(0).getSiteId());
        assertEquals(Long.valueOf(100), records.get(3).getSiteId());
    }

    private static class ListCollector implements Collector<ODSReviewRecord> {
        private final List<ODSReviewRecord> records;

        private ListCollector(List<ODSReviewRecord> records) {
            this.records = records;
        }

        @Override
        public void collect(ODSReviewRecord record) {
            records.add(record);
        }

        @Override
        public void close() {
        }
    }
}
