package com.lingyun.business.geekbiGoods.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.lingyun.business.common.util.JsonFileSource;
import com.lingyun.business.common.util.JsonUtil;
import com.lingyun.business.geekbiGoods.model.DWDGeekbiGoodsRecord;
import com.lingyun.business.geekbiGoods.model.ODSGeekbiGoodsRecord;
import com.lingyun.business.geekbiGoods.sink.DorisBatchSink;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GeekbiGoodsProcessorTest {

    @Test
    public void sourcePayloadProducesOneOdsRecordAndDailyDwdRecords() throws Exception {
        ODSGeekbiGoodsRecord odsRecord = new ODSGeekbiGoodsProcessor().map(sampleJson());

        assertNotNull(odsRecord);
        assertEquals(Long.valueOf(601099511938352L), odsRecord.getGoodsId());
        assertEquals(Integer.valueOf(48), odsRecord.getSiteId());
        assertNotNull(odsRecord.getDate());
        assertTrue(odsRecord.getRankings().isObject());
        assertTrue(odsRecord.getSite().isObject());
        assertTrue(odsRecord.getStats().isArray());
        assertTrue(odsRecord.getGoods().isObject());
        assertEquals(2, odsRecord.getHistory().size());

        List<DWDGeekbiGoodsRecord> dwdRecords = new ArrayList<>();
        new DWDGeekbiGoodsProcessor().flatMap(odsRecord, new ListCollector(dwdRecords));

        assertEquals(2, dwdRecords.size());

        DWDGeekbiGoodsRecord first = dwdRecords.get(0);
        assertEquals(Long.valueOf(601099511938352L), first.getGoodsId());
        assertEquals(Integer.valueOf(48), first.getSiteId());
        assertEquals("2026-05-05", first.getDate());
        assertEquals(Long.valueOf(211L), first.getRegionId());
        assertEquals("source-record-1", first.getSourceRecordId());
        assertEquals("36210411699", first.getMallId());
        assertEquals("商品中文名", first.getGoodsNameCn());
        assertEquals("Product name", first.getGoodsNameEn());
        assertEquals(Long.valueOf(17056L), first.getCatId());
        assertEquals("USD", first.getCurrency());
        assertEquals(Integer.valueOf(100), first.getCents());
        assertEquals(new BigDecimal("899000.0000"), first.getSales());
        assertEquals(new BigDecimal("8.6300"), first.getMinPrice());
        assertEquals("2026-05-05 00:13:41", first.getHistoryCreateTime());
        assertEquals("2023-03-03 00:44:10", first.getOnSaleTime());
        assertTrue(first.getSkuInfo().isArray());
        assertEquals(1, first.getSkuInfo().size());

        DWDGeekbiGoodsRecord second = dwdRecords.get(1);
        assertEquals("2026-05-06", second.getDate());
        assertEquals(Long.valueOf(101230L), second.getSold());
        assertEquals(new BigDecimal("11057.7000"), second.getDaySales());
    }

    @Test
    public void sourceRecordsSerializeWithJsonColumnsAndDorisColumnNames() throws Exception {
        ODSGeekbiGoodsRecord odsRecord = new ODSGeekbiGoodsProcessor().map(sampleJson());
        JsonNode odsJson = JsonUtil.parseJson(DorisBatchSink.odsRecordToJson(odsRecord));

        assertNotNull(odsJson);
        assertTrue(odsJson.path("rankings").isObject());
        assertTrue(odsJson.path("history").isArray());

        List<DWDGeekbiGoodsRecord> dwdRecords = new ArrayList<>();
        new DWDGeekbiGoodsProcessor().flatMap(odsRecord, new ListCollector(dwdRecords));
        JsonNode dwdJson = JsonUtil.parseJson(DorisBatchSink.dwdRecordToJson(dwdRecords.get(0)));

        assertNotNull(dwdJson);
        assertEquals("601099511938352", dwdJson.path("goodsId").asText());
        assertTrue(dwdJson.path("catIds").isArray());
        assertTrue(dwdJson.path("catItems").isArray());
        assertTrue(dwdJson.path("skuInfo").isArray());
        assertEquals("2026-05-05 00:13:41", dwdJson.path("historyCreateTime").asText());
    }

    @Test
    public void invalidOrIncompletePayloadIsSkipped() throws Exception {
        assertEquals(null, new ODSGeekbiGoodsProcessor().map(""));
        assertEquals(null, new ODSGeekbiGoodsProcessor().map("{\"code\":0,\"data\":{}}"));
        assertEquals(null, new ODSGeekbiGoodsProcessor().map("{invalid"));
    }

    @Test
    public void fullGeekbiGoodsSampleExpandsAllHistoryDays() throws Exception {
        String json = JsonFileSource.readJsonContent("samples/geekbiGoods.json");
        ODSGeekbiGoodsRecord odsRecord = new ODSGeekbiGoodsProcessor().map(json);
        List<DWDGeekbiGoodsRecord> dwdRecords = new ArrayList<>();

        new DWDGeekbiGoodsProcessor().flatMap(odsRecord, new ListCollector(dwdRecords));

        assertEquals(30, dwdRecords.size());
        assertEquals("2026-05-05", dwdRecords.get(0).getDate());
        assertEquals("2026-06-03", dwdRecords.get(29).getDate());
        assertEquals(Long.valueOf(131267L), dwdRecords.get(29).getSold());
        assertEquals(new BigDecimal("1180090.3300"), dwdRecords.get(29).getSales());
        assertEquals(2, dwdRecords.get(29).getSkuInfo().size());
    }

    private static String sampleJson() {
        return "{"
                + "\"code\":0,"
                + "\"data\":{"
                + "\"rankings\":{\"siteDayRank\":303,\"isNewHotProduct\":false},"
                + "\"site\":{\"id\":48,\"regionId\":211,\"currency\":\"USD\",\"exchangeRate\":0.1392,\"cents\":100},"
                + "\"stats\":[{\"percentage\":19,\"name\":\"美区\",\"countries\":[]}],"
                + "\"goods\":{"
                + "\"id\":\"source-record-1\",\"regionId\":211,\"goodsId\":\"601099511938352\","
                + "\"mallId\":\"36210411699\",\"thumbnail\":\"image\",\"thumbnailCn\":\"image-cn\","
                + "\"catIds\":[17056,15945],\"optId\":null,"
                + "\"catItems\":["
                + "{\"catId\":15945,\"catName\":\"一级类目\",\"isLeaf\":false},"
                + "{\"catId\":17056,\"catName\":\"叶子类目\",\"isLeaf\":true}"
                + "],"
                + "\"goodsName\":\"商品名\",\"goodsNameCn\":\"商品中文名\",\"goodsNameEn\":\"Product name\","
                + "\"sku\":\"[{\\\"skuId\\\":17592187989292,\\\"stock\\\":558}]\","
                + "\"status\":1,\"hostingMode\":1,"
                + "\"onSaleTime\":\"2023-03-03T00:44:10.419+08:00\","
                + "\"createTime\":\"2025-08-27T06:21:04.791+00:00\","
                + "\"updateTime\":\"2026-06-03T15:40:54.747+00:00\""
                + "},"
                + "\"history\":["
                + historyJson("100000", "899000.0", "0", "0.0",
                "\"2026-05-05T00:13:41.078+08:00\"")
                + ","
                + historyJson("101230", "910057.7", "1230", "11057.7",
                "\"2026-05-06T00:00:00.000+08:00\"")
                + "]"
                + "}"
                + "}";
    }

    private static String historyJson(
            String sold,
            String sales,
            String daySold,
            String daySales,
            String createTime) {
        return "{"
                + "\"id\":null,\"regionId\":211,\"goodsId\":\"601099511938352\","
                + "\"sold\":" + sold + ",\"sales\":" + sales + ","
                + "\"minPrice\":8.63,\"maxPrice\":9.71,\"quantity\":1623,"
                + "\"goodsScore\":4.7,\"reviewNum\":2831,"
                + "\"daySold\":" + daySold + ",\"weekSold\":8610,\"monthSold\":36900,"
                + "\"daySales\":" + daySales + ",\"weekSales\":77403.9,\"monthSales\":331731.0,"
                + "\"daySoldRate\":null,\"weekSoldRate\":null,\"monthSoldRate\":null,"
                + "\"daySalesRate\":null,\"weekSalesRate\":null,\"monthSalesRate\":null,"
                + "\"dayClickNum\":0,\"weekClickNum\":0,\"monthClickNum\":17,"
                + "\"dayExposureNum\":7,\"weekExposureNum\":62,\"monthExposureNum\":252,"
                + "\"dayClickRate\":0.0,\"weekClickRate\":0.0,\"monthClickRate\":0.0675,"
                + "\"dayClickGrowthRate\":0.0,\"weekClickGrowthRate\":-1.0,\"monthClickGrowthRate\":-0.575,"
                + "\"dayExposureGrowthRate\":-0.3636,\"weekExposureGrowthRate\":0.3478,"
                + "\"monthExposureGrowthRate\":-0.2075,"
                + "\"createTime\":" + createTime + ",\"updateTime\":" + createTime
                + "}";
    }

    private static class ListCollector implements Collector<DWDGeekbiGoodsRecord> {
        private final List<DWDGeekbiGoodsRecord> records;

        private ListCollector(List<DWDGeekbiGoodsRecord> records) {
            this.records = records;
        }

        @Override
        public void collect(DWDGeekbiGoodsRecord record) {
            records.add(record);
        }

        @Override
        public void close() {
        }
    }
}
