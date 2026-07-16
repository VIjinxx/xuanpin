package com.lingyun.business.geekbiGoods.job;

import com.lingyun.business.geekbiGoods.model.DWDGeekbiGoodsRecord;
import com.lingyun.business.geekbiGoods.model.ODSGeekbiGoodsRecord;
import com.lingyun.business.geekbiGoods.sink.DorisBatchSink;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FlinkGeekbiGoodsJobTest {

    @Test
    public void jobUsesGeekbiGoodsKafkaTopicAndDedicatedDorisTables() {
        assertEquals("geekbiGoods", FlinkGeekbiGoodsJob.SOURCE_TOPIC);
        assertEquals("geekbi-goods", FlinkGeekbiGoodsJob.CONSUMER_GROUP);
        assertEquals("ods_geekbi_goods_raw", DorisBatchSink.ODS_TABLE);
        assertEquals("dwd_geekbi_goods_daily", DorisBatchSink.DWD_TABLE);
    }

    @Test
    public void requiredKeysProtectOdsAndDwdDorisKeys() {
        ODSGeekbiGoodsRecord ods = new ODSGeekbiGoodsRecord();
        ods.setGoodsId(1L);
        ods.setSiteId(48);
        ods.setDate("2026-06-10");
        assertTrue(FlinkGeekbiGoodsJob.hasRequiredKeys(ods));
        ods.setSiteId(null);
        assertFalse(FlinkGeekbiGoodsJob.hasRequiredKeys(ods));

        DWDGeekbiGoodsRecord dwd = new DWDGeekbiGoodsRecord();
        dwd.setGoodsId(1L);
        dwd.setSiteId(48);
        dwd.setDate("2026-06-03");
        assertTrue(FlinkGeekbiGoodsJob.hasRequiredKeys(dwd));
        dwd.setDate(null);
        assertFalse(FlinkGeekbiGoodsJob.hasRequiredKeys(dwd));
    }
}
