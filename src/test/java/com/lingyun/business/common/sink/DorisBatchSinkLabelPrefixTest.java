package com.lingyun.business.common.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.sink.writer.WriteMode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DorisBatchSinkLabelPrefixTest {

    @Test
    public void usesRequestedShortCategoryLabelPrefixes() {
        assertEquals("lf_run123_cat_ods",
                DorisBatchSink.createLabelPrefix("ods_category_raw", null, "run123"));
        assertEquals("lf_run123_cat_dwd",
                DorisBatchSink.createLabelPrefix("dwd_category_base", null, "run123"));
    }

    @Test
    public void shortensKnownDorisTableNames() {
        assertEquals("lf_run123_goods_ods",
                DorisBatchSink.createLabelPrefix("ods_goods_raw", null, "run123"));
        assertEquals("lf_run123_site_dwd",
                DorisBatchSink.createLabelPrefix("dwd_site_base", null, "run123"));
        assertEquals("lf_run123_mall_ods",
                DorisBatchSink.createLabelPrefix("ods_malls_raw", null, "run123"));
        assertEquals("lf_run123_review_dwd",
                DorisBatchSink.createLabelPrefix("dwd_review_base", null, "run123"));
    }

    @Test
    public void differentFreshRunsUseDifferentDefaultPrefixes() {
        String firstRun = DorisBatchSink.createLabelPrefix("ods_goods_raw", null, "run123");
        String secondRun = DorisBatchSink.createLabelPrefix("ods_goods_raw", null, "run456");

        assertEquals("lf_run123_goods_ods", firstRun);
        assertEquals("lf_run456_goods_ods", secondRun);
        assertFalse(firstRun.equals(secondRun));
    }

    @Test
    public void configuredPrefixRemainsStableAcrossRuns() {
        assertEquals("lf_job_cat_ods",
                DorisBatchSink.createLabelPrefix("ods_category_raw", "lingfeng_job", "run123"));
        assertEquals("lf_job_cat_ods",
                DorisBatchSink.createLabelPrefix("ods_category_raw", "lingfeng_job", "run456"));
    }

    @Test
    public void keepsUnknownTablesWithinDorisLabelPrefixLimit() {
        String labelPrefix = DorisBatchSink.createLabelPrefix(
                "ods_extremely_long_category_dimension_with_many_nested_business_words_and_extra_suffix_raw",
                null,
                "run123");

        assertTrue(labelPrefix.startsWith("lf_"));
        assertTrue(labelPrefix.length() <= 120);
    }

    @Test
    public void localModeKeepsStreamingWriteModeForExactlyOnce() {
        String previous = System.getProperty("lingyun.job.local");
        try {
            System.setProperty("lingyun.job.local", "true");

            DorisExecutionOptions options = DorisBatchSink.createExecutionOptions("ods_review_raw");

            assertFalse(options.enableBatchMode());
            assertEquals(WriteMode.STREAM_LOAD, options.getWriteMode());
            assertEquals(Boolean.TRUE, options.enabled2PC());
            assertEquals(DorisBatchSink.createLabelPrefix("ods_review_raw"), options.getLabelPrefix());
        } finally {
            if (previous == null) {
                System.clearProperty("lingyun.job.local");
            } else {
                System.setProperty("lingyun.job.local", previous);
            }
        }
    }
}
