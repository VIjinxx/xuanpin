package com.lingyun.business.goodsDetail.goods.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.lingyun.business.common.model.goods.DWDGoodsRecord;
import com.lingyun.business.common.model.goods.ODSGoodsRecord;
import com.lingyun.business.common.util.JsonFileSource;
import com.lingyun.business.common.util.JsonUtil;
import com.lingyun.business.goodsDetail.goods.sink.DorisBatchSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ODSGoodsProcessorTest {

    @Test
    public void goodsDetailSampleMapsBenefitTextAsJsonArray() throws Exception {
        String jsonContent = JsonFileSource.readJsonContent("samples/商品详情页.json");
        ODSGoodsProcessor processor = new ODSGoodsProcessor();
        processor.open(new Configuration());

        ODSGoodsRecord record;
        try {
            record = processor.map(jsonContent);
        } finally {
            processor.close();
        }

        assertNotNull(record);
        assertEquals(Long.valueOf(606201362396258L), record.getGoodsId());
        assertEquals(Long.valueOf(100L), record.getSiteId());
        assertEquals(Long.valueOf(29L), record.getSalesNum());
        assertTrue(record.getSalesTipText().isArray());
        assertEquals("By ", record.getSalesTipText().get(0).asText());
        assertEquals(
                "https://img.kwcdn.com/product/open/b039001ebb434d819d7ebda8b9bfec6c-goods.jpeg",
                record.getThumbUrl());
        assertNotNull(record.getBenefitText());
        assertTrue(record.getBenefitText().isArray());
        assertFalse(record.getBenefitText().isEmpty());
        assertEquals("平台包邮", record.getBenefitText().get(0).path("text").asText());

        JsonNode odsJson = JsonUtil.parseJson(DorisBatchSink.odsRecordToJson(record));
        assertNotNull(odsJson);
        assertEquals(record.getThumbUrl(), odsJson.path("thumbUrl").asText());
        assertTrue(odsJson.path("salesTipText").isArray());
        assertEquals("By ", odsJson.path("salesTipText").get(0).asText());
        assertTrue(odsJson.path("benefitText").isArray());
        assertEquals("平台包邮", odsJson.path("benefitText").get(0).path("text").asText());

        DWDGoodsProcessor dwdProcessor = new DWDGoodsProcessor();
        dwdProcessor.open(new Configuration());
        try {
            DWDGoodsRecord dwdRecord = dwdProcessor.map(record);
            assertEquals(record.getThumbUrl(), dwdRecord.getThumbUrl());
            assertEquals(Long.valueOf(29L), dwdRecord.getSalesNum());

            JsonNode dwdJson = JsonUtil.parseJson(DorisBatchSink.dwdRecordToJson(dwdRecord));
            assertNotNull(dwdJson);
            assertEquals(record.getThumbUrl(), dwdJson.path("thumbUrl").asText());
            assertEquals(29L, dwdJson.path("salesNum").asLong());
        } finally {
            dwdProcessor.close();
        }
    }

    @Test
    public void orderedSourceMappingsGenerateOneCompleteGoodsRecord() throws Exception {
        String json = "{"
                + "\"goodsId\":9001,"
                + "\"goods\":{\"goodsId\":9002,\"thumbUrl\":\"primary-thumb\"},"
                + "\"sku\":[{\"thumbUrl\":\"sku-thumb\"}],"
                + "\"formatSkuData\":{\"skuInfos\":{\"sku-a\":{\"thumbUrl\":\"format-thumb\"}}},"
                + "\"reviewStore\":{"
                + "\"pageReviewListMap\":{\"0\":["
                + "{\"goodsId\":1001,\"opList\":[{\"linkUrl\":\"page-link-1\",\"status\":11}]},"
                + "{\"goodsId\":1002,\"opList\":[{\"linkUrl\":\"page-link-2\",\"status\":12}]}"
                + "]},"
                + "\"commentList\":["
                + "{\"goodsId\":2001,\"opList\":[{\"linkUrl\":\"comment-link-1\",\"status\":21}]}"
                + "]"
                + "},"
                + "\"moduleMap\":{\"shareModule\":{\"data\":{\"share\":{\"linkUrl\":\"share-link\"}}}},"
                + "\"webLayoutData\":{\"headerData\":{\"region\":{\"regionInfo\":{\"siteId\":100,"
                + "\"selectedLang\":\"en\",\"selectedCurrency\":\"USD\"}}}},"
                + "\"localInfo\":{\"timezone\":\"Asia/Shanghai\"},"
                + "\"lang\":\"en\",\"userAgent\":\"test-agent\",\"serverTime\":\"999999\""
                + "}";

        ODSGoodsProcessor processor = new ODSGoodsProcessor();
        List<ODSGoodsRecord> records = new ArrayList<>();
        processor.open(new Configuration());
        try {
            processor.flatMap(json, new ListCollector(records));
        } finally {
            processor.close();
        }

        assertEquals(1, records.size());

        ODSGoodsRecord record = records.get(0);
        assertEquals(Long.valueOf(9001), record.getGoodsId());
        assertEquals("page-link-1", record.getLinkUrl());
        assertEquals("primary-thumb", record.getThumbUrl());
        assertEquals(Long.valueOf(100), record.getSiteId());
        assertEquals("test-agent", record.getUserAgent());
        assertEquals("Asia/Shanghai", record.getTimezone());
    }

    @Test
    public void goodsDetailMapsRequestedOdsFields() throws Exception {
        String json = "{"
                + "\"goodsId\":6201,"
                + "\"sku\":[{\"skuExt\":{\"business_reduction\":\"660\"}}],"
                + "\"goods\":{\"sellerType\":2},"
                + "\"query\":{\"search_key\":\"Men's Earmuffs\"},"
                + "\"review\":{"
                + "\"reviewData\":{\"expParams\":{\"sortTypeStyle\":\"1\"}},"
                + "\"reviewScore\":4.8"
                + "},"
                + "\"moduleMap\":{\"mallModule\":{\"data\":{\"mallData\":{\"mallTags\":[{\"tagCode\":70}]}}}}"
                + "}";

        ODSGoodsProcessor processor = new ODSGoodsProcessor();
        processor.open(new Configuration());
        try {
            ODSGoodsRecord record = processor.map(json);

            assertNotNull(record);
            assertEquals("660", record.getBusinessReduction());
            assertEquals(Long.valueOf(2), record.getSellerType());
            assertEquals("Men's Earmuffs", record.getSearchKey());
            assertEquals(Long.valueOf(1), record.getSortTypeStyle());
            assertEquals("4.8", record.getReviewScore());
            assertEquals(Long.valueOf(70), record.getTagCode());

            JsonNode odsJson = JsonUtil.parseJson(DorisBatchSink.odsRecordToJson(record));
            assertNotNull(odsJson);
            assertEquals("660", odsJson.path("businessReduction").asText());
            assertEquals(2L, odsJson.path("sellerType").asLong());
            assertEquals("Men's Earmuffs", odsJson.path("searchKey").asText());
            assertEquals(1L, odsJson.path("sortTypeStyle").asLong());
            assertEquals("4.8", odsJson.path("reviewScore").asText());
            assertEquals(70L, odsJson.path("tagCode").asLong());
        } finally {
            processor.close();
        }
    }

    @Test
    public void thumbUrlFallsBackAcrossOrderedSources() throws Exception {
        ODSGoodsProcessor processor = new ODSGoodsProcessor();
        processor.open(new Configuration());
        try {
            ODSGoodsRecord skuFallback = processor.map("{"
                    + "\"goods\":{\"goodsId\":4001,\"thumbUrl\":\"\"},"
                    + "\"sku\":[{\"thumbUrl\":\"sku-thumb\"}],"
                    + "\"formatSkuData\":{\"skuInfos\":{\"sku-a\":{\"thumbUrl\":\"format-thumb\"}}}"
                    + "}");
            assertNotNull(skuFallback);
            assertEquals("sku-thumb", skuFallback.getThumbUrl());

            ODSGoodsRecord mapFallback = processor.map("{"
                    + "\"goods\":{\"goodsId\":4002,\"thumbUrl\":null},"
                    + "\"sku\":[{\"thumbUrl\":\"\"}],"
                    + "\"formatSkuData\":{\"skuInfos\":{"
                    + "\"sku-a\":{\"thumbUrl\":\"\"},"
                    + "\"sku-b\":{\"thumbUrl\":\"format-thumb\"}"
                    + "}}"
                    + "}");
            assertNotNull(mapFallback);
            assertEquals("format-thumb", mapFallback.getThumbUrl());
        } finally {
            processor.close();
        }
    }

    @Test
    public void titleSkipsTemuAndFallsBackToNextSource() throws Exception {
        ODSGoodsProcessor processor = new ODSGoodsProcessor();
        processor.open(new Configuration());
        try {
            ODSGoodsRecord fallback = processor.map("{"
                    + "\"goodsId\":4101,"
                    + "\"goods\":{\"goodsName\":\"Temu\"},"
                    + "\"reviewStore\":{\"similarReview\":{\"reviewInfoList\":["
                    + "{\"goodsInfo\":{\"goodsName\":\"Actual product title\"}}"
                    + "]}}"
                    + "}");
            assertNotNull(fallback);
            assertEquals("Actual product title", fallback.getTitle());

            ODSGoodsRecord firstSource = processor.map("{"
                    + "\"goodsId\":4102,"
                    + "\"goods\":{\"goodsName\":\"First product title\"},"
                    + "\"reviewStore\":{\"similarReview\":{\"reviewInfoList\":["
                    + "{\"goodsInfo\":{\"goodsName\":\"Second product title\"}}"
                    + "]}}"
                    + "}");
            assertNotNull(firstSource);
            assertEquals("First product title", firstSource.getTitle());
        } finally {
            processor.close();
        }
    }

    @Test
    public void emptyArraysAndObjectsFallBackToLaterSources() throws Exception {
        ODSGoodsProcessor processor = new ODSGoodsProcessor();
        processor.open(new Configuration());
        try {
            ODSGoodsRecord emptyArray = processor.map("{"
                    + "\"goods\":{\"goodsId\":5001,\"mallId\":[]},"
                    + "\"mall\":{\"mallData\":{\"mallId\":\"mall-from-second-source\"}}"
                    + "}");
            assertNotNull(emptyArray);
            assertEquals("mall-from-second-source", emptyArray.getMallId());

            ODSGoodsRecord emptyObject = processor.map("{"
                    + "\"goods\":{\"goodsId\":5002,\"mallId\":{}},"
                    + "\"mall\":{\"mallData\":{\"mallId\":\"mall-from-second-source\"}}"
                    + "}");
            assertNotNull(emptyObject);
            assertEquals("mall-from-second-source", emptyObject.getMallId());
        } finally {
            processor.close();
        }
    }

    @Test
    public void valuesThatCannotConvertFallBackToLaterSources() throws Exception {
        ODSGoodsProcessor processor = new ODSGoodsProcessor();
        processor.open(new Configuration());
        try {
            ODSGoodsRecord record = processor.map("{"
                    + "\"goodsId\":5101,"
                    + "\"reviewStore\":{"
                    + "\"similarReview\":{\"reviewNum\":\"not-a-number\"},"
                    + "\"reviewNum\":25"
                    + "}"
                    + "}");
            assertNotNull(record);
            assertEquals(Long.valueOf(25), record.getReviewNum());
        } finally {
            processor.close();
        }
    }

    @Test
    public void goodsDetailMappingMatchesExcelOrderedSources() throws Exception {
        InputStream inputStream = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream("mappingFile/goodsDetail/ods_goods_detail_goods_mapping.json");
        assertNotNull(inputStream);

        JsonNode mapping;
        try (InputStream stream = inputStream) {
            mapping = JsonUtil.getObjectMapper().readTree(stream);
        }

        assertEquals(
                Arrays.asList(
                        "goods.thumbUrl",
                        "sku[0].thumbUrl",
                        "formatSkuData.skuInfos.value.thumbUrl",
                        "reviewStore.similarReview.reviewInfoList[i].goodsInfo.thumbUrl",
                        "review.reviewData.similarReview.reviewInfoList[i].goodsInfo.thumbUrl"
                ),
                JsonUtil.getObjectMapper().convertValue(
                        mapping.path("thumbUrl"),
                        JsonUtil.getObjectMapper().getTypeFactory()
                                .constructCollectionType(List.class, String.class)
                )
        );
        assertEquals(
                Arrays.asList("goods.mallId", "mall.mallData.mallId", "moduleMap.mallModule.data.mallData.mallId"),
                JsonUtil.getObjectMapper().convertValue(
                        mapping.path("mallId"),
                        JsonUtil.getObjectMapper().getTypeFactory()
                                .constructCollectionType(List.class, String.class)
                )
        );
        assertEquals("mall.mallData.goodsSalesNumUnit[0]", mapping.path("salesNum").asText());
    }

    @Test
    public void semiManagedUsesCurrentIsLocalGoodsMapping() throws Exception {
        ODSGoodsProcessor processor = new ODSGoodsProcessor();
        processor.open(new Configuration());
        try {
            ODSGoodsRecord fullManaged = processor.map("{\"goods\":{\"goodsId\":3001,\"isLocalGoods\":0}}");
            assertNotNull(fullManaged);
            assertEquals(Long.valueOf(0), fullManaged.getIsLocalGoods());
            assertEquals(Boolean.FALSE, fullManaged.getSemiManaged());

            ODSGoodsRecord semiManaged = processor.map("{\"goods\":{\"goodsId\":3002,\"isLocalGoods\":1}}");
            assertNotNull(semiManaged);
            assertEquals(Long.valueOf(1), semiManaged.getIsLocalGoods());
            assertEquals(Boolean.TRUE, semiManaged.getSemiManaged());
        } finally {
            processor.close();
        }
    }

    private static class ListCollector implements Collector<ODSGoodsRecord> {
        private final List<ODSGoodsRecord> records;

        private ListCollector(List<ODSGoodsRecord> records) {
            this.records = records;
        }

        @Override
        public void collect(ODSGoodsRecord record) {
            records.add(record);
        }

        @Override
        public void close() {
        }
    }
}
