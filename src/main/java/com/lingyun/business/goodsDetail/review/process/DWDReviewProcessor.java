package com.lingyun.business.goodsDetail.review.process;

import com.lingyun.business.common.model.review.DWDReviewRecord;
import com.lingyun.business.common.model.review.ODSReviewRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DWD层评论数据处理器。
 * 当前评论基础表字段来自ODS评论明细，先做同名字段透传。
 */
public class DWDReviewProcessor extends RichMapFunction<ODSReviewRecord, DWDReviewRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DWDReviewProcessor.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOGGER.info("DWDReviewProcessor 初始化完成");
    }

    @Override
    public void close() throws Exception {
        LOGGER.info("DWDReviewProcessor 资源已释放");
        super.close();
    }

    @Override
    public DWDReviewRecord map(ODSReviewRecord odsRecord) {
        if (odsRecord == null) {
            return null;
        }

        DWDReviewRecord dwdRecord = new DWDReviewRecord();
        dwdRecord.setReviewId(odsRecord.getReviewId());
        dwdRecord.setSiteId(odsRecord.getSiteId());
        dwdRecord.setDate(odsRecord.getDate());
        dwdRecord.setLang(odsRecord.getLang());
        dwdRecord.setGoodsId(odsRecord.getGoodsId());
        dwdRecord.setSkuId(odsRecord.getSkuId());
        dwdRecord.setComment(odsRecord.getComment());
        dwdRecord.setScore(odsRecord.getScore());
        dwdRecord.setSpecs(odsRecord.getSpecs());
        dwdRecord.setTime(odsRecord.getTime());
        dwdRecord.setConcatTimeLang(odsRecord.getConcatTimeLang());
        dwdRecord.setConcatRichText(odsRecord.getConcatRichText());
        dwdRecord.setAvatar(odsRecord.getAvatar());
        dwdRecord.setName(odsRecord.getName());
        dwdRecord.setProfileLinkUrl(odsRecord.getProfileLinkUrl());
        dwdRecord.setPictures(odsRecord.getPictures());
        dwdRecord.setOpList(odsRecord.getOpList());
        dwdRecord.setViewMoreList(odsRecord.getViewMoreList());
        dwdRecord.setReviewLang(odsRecord.getReviewLang());
        dwdRecord.setList(odsRecord.getList());
        dwdRecord.setInBlacklist(odsRecord.getInBlacklist());
        return dwdRecord;
    }
}
