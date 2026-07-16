package com.lingyun.business.goodsDetail.mall.process;

import com.lingyun.business.common.model.mall.DWDMallsRecord;
import com.lingyun.business.common.model.mall.ODSMallsRecord;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MallGoodsNumCleanTest {

    @Test
    public void goodsDetailMallCleansGoodsNumBeforeDorisWrite() throws Exception {
        ODSMallsProcessor odsProcessor = new ODSMallsProcessor();
        DWDMallsProcessor dwdProcessor = new DWDMallsProcessor();
        odsProcessor.open(new Configuration());
        dwdProcessor.open(new Configuration());
        try {
            ODSMallsRecord odsRecord = odsProcessor.map(
                    "{\"mall\":{\"mallData\":{\"goodsNum\":\"1,324 商品\"}}}");
            assertEquals("1324", odsRecord.getGoodsNum());

            DWDMallsRecord dwdRecord = dwdProcessor.map(odsRecord);
            assertEquals("1324", dwdRecord.getGoodsNum());
        } finally {
            dwdProcessor.close();
            odsProcessor.close();
        }
    }

    @Test
    public void storeDetailMallCleansGoodsNumBeforeDorisWrite() throws Exception {
        com.lingyun.business.storeDetail.mall.process.ODSMallsProcessor odsProcessor =
                new com.lingyun.business.storeDetail.mall.process.ODSMallsProcessor();
        com.lingyun.business.storeDetail.mall.process.DWDMallsProcessor dwdProcessor =
                new com.lingyun.business.storeDetail.mall.process.DWDMallsProcessor();
        odsProcessor.open(new Configuration());
        dwdProcessor.open(new Configuration());
        try {
            ODSMallsRecord odsRecord = odsProcessor.map(
                    "{\"page\":{\"mallInfo\":{\"goodsNum\":\"1,324 商品\"}}}");
            assertEquals("1324", odsRecord.getGoodsNum());

            DWDMallsRecord dwdRecord = dwdProcessor.map(odsRecord);
            assertEquals("1324", dwdRecord.getGoodsNum());
        } finally {
            dwdProcessor.close();
            odsProcessor.close();
        }
    }

    @Test
    public void dwdMallCleansGoodsNumEvenWhenOdsRecordWasCreatedDirectly() throws Exception {
        ODSMallsRecord odsRecord = new ODSMallsRecord();
        odsRecord.setGoodsNum("1,324 商品");

        DWDMallsProcessor goodsDetailProcessor = new DWDMallsProcessor();
        goodsDetailProcessor.open(new Configuration());
        try {
            assertEquals("1324", goodsDetailProcessor.map(odsRecord).getGoodsNum());
        } finally {
            goodsDetailProcessor.close();
        }

        com.lingyun.business.storeDetail.mall.process.DWDMallsProcessor storeDetailProcessor =
                new com.lingyun.business.storeDetail.mall.process.DWDMallsProcessor();
        storeDetailProcessor.open(new Configuration());
        try {
            assertEquals("1324", storeDetailProcessor.map(odsRecord).getGoodsNum());
        } finally {
            storeDetailProcessor.close();
        }
    }
}
