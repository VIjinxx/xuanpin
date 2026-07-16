package com.lingyun.business.goodsDetail.review.job;

import com.lingyun.business.common.model.review.DWDReviewRecord;
import com.lingyun.business.common.model.review.ODSReviewRecord;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FlinkReviewJobTest {

    @Test
    public void odsRequiredKeysRejectMissingReviewIdSiteIdOrDate() {
        assertTrue(FlinkReviewJob.hasRequiredKeys(validOdsRecord()));

        ODSReviewRecord missingReviewId = validOdsRecord();
        missingReviewId.setReviewId(null);
        assertFalse(FlinkReviewJob.hasRequiredKeys(missingReviewId));

        ODSReviewRecord blankReviewId = validOdsRecord();
        blankReviewId.setReviewId(" ");
        assertFalse(FlinkReviewJob.hasRequiredKeys(blankReviewId));

        ODSReviewRecord missingSiteId = validOdsRecord();
        missingSiteId.setSiteId(null);
        assertFalse(FlinkReviewJob.hasRequiredKeys(missingSiteId));

        ODSReviewRecord missingDate = validOdsRecord();
        missingDate.setDate(null);
        assertFalse(FlinkReviewJob.hasRequiredKeys(missingDate));
    }

    @Test
    public void dwdRequiredKeysRejectMissingReviewIdSiteIdOrDate() {
        assertTrue(FlinkReviewJob.hasRequiredKeys(validDwdRecord()));

        DWDReviewRecord missingReviewId = validDwdRecord();
        missingReviewId.setReviewId(null);
        assertFalse(FlinkReviewJob.hasRequiredKeys(missingReviewId));

        DWDReviewRecord blankReviewId = validDwdRecord();
        blankReviewId.setReviewId("");
        assertFalse(FlinkReviewJob.hasRequiredKeys(blankReviewId));

        DWDReviewRecord missingSiteId = validDwdRecord();
        missingSiteId.setSiteId(null);
        assertFalse(FlinkReviewJob.hasRequiredKeys(missingSiteId));

        DWDReviewRecord missingDate = validDwdRecord();
        missingDate.setDate(" ");
        assertFalse(FlinkReviewJob.hasRequiredKeys(missingDate));
    }

    private static ODSReviewRecord validOdsRecord() {
        ODSReviewRecord record = new ODSReviewRecord();
        record.setReviewId("review-1");
        record.setSiteId(100L);
        record.setDate("2026-05-28");
        return record;
    }

    private static DWDReviewRecord validDwdRecord() {
        DWDReviewRecord record = new DWDReviewRecord();
        record.setReviewId("review-1");
        record.setSiteId(100L);
        record.setDate("2026-05-28");
        return record;
    }
}
