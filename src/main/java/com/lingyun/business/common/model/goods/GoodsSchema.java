package com.lingyun.business.common.model.goods;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 商品表字段定义，来自原始商品表、基础商品表、商品聚合表设计文档。
 */
public final class GoodsSchema {
    private static final List<FieldDef> ODS_FIELDS = Collections.unmodifiableList(Arrays.asList(
            field("goodsId", GoodsFieldType.INTEGER, "integer", "商品id"),
            field("siteId", GoodsFieldType.INTEGER, "integer", "商品采集站点"),
            field("mallId", GoodsFieldType.STRING, "string", "商品所属店铺id"),
            field("sellerType", GoodsFieldType.INTEGER, "integer", "商家类型编码"),
            field("currentSkuId", GoodsFieldType.STRING, "string", "当前skuid"),
            field("defaultSkuId", GoodsFieldType.STRING, "string", "默认的skuid"),
            field("selectedLang", GoodsFieldType.STRING, "string", "选择的语言"),
            field("lang", GoodsFieldType.STRING, "string", "语言"),
            field("selectedCurrency", GoodsFieldType.STRING, "string", "选择的货币单位"),
            field("optId", GoodsFieldType.INTEGER, "integer", "前端类目id"),
            field("optType", GoodsFieldType.INTEGER, "integer", "optType"),
            field("optLevel", GoodsFieldType.INTEGER, "integer", "optLevel"),
            field("catId", GoodsFieldType.INTEGER, "integer", "商家后台类目id"),
            field("catInfo", GoodsFieldType.JSON, "object", "商家后台类目的多层级id信息"),
            field("title", GoodsFieldType.STRING, "string", "商品标题"),
            field("goodsName", GoodsFieldType.STRING, "string", "商品标题"),
            field("linkUrl", GoodsFieldType.STRING, "string", "商品链接"),
            field("activityType", GoodsFieldType.INTEGER, "integer", "活动类型"),
            field("sideSalesTip", GoodsFieldType.STRING, "string", "销售贴士"),
            field("salesTip", GoodsFieldType.STRING, "string", "销售贴士"),
            field("salesTipText", GoodsFieldType.JSON, "array[string]", "销售文本列表"),
            field("salesTipTextList", GoodsFieldType.JSON, "array[string]", "销售文本列表"),
            field("businessReduction", GoodsFieldType.STRING, "string", "商家承担优惠金额"),
            field("isOnsale", GoodsFieldType.INTEGER, "integer", "是否在售"),
            field("isShowMarketPrice", GoodsFieldType.BOOLEAN, "boolean", "是否展示原价"),
            field("isSkc", GoodsFieldType.BOOLEAN, "boolean", "是否为skc"),
            field("qty", GoodsFieldType.INTEGER, "integer", "qty"),
            field("status", GoodsFieldType.INTEGER, "integer", "status"),
            field("soldOutSubscribeStatus", GoodsFieldType.INTEGER, "integer", "soldOutSubscribeStatus"),
            field("salesNum", GoodsFieldType.INTEGER, "integer", "销售数量"),
            field("soldQuantity", GoodsFieldType.INTEGER, "integer", "销售数量"),
            field("reviewNum", GoodsFieldType.INTEGER, "integer", "评论数量"),
            field("commentNumTips", GoodsFieldType.STRING, "string", "评论数量提示"),
            field("sortTypeStyle", GoodsFieldType.INTEGER, "integer", "评价列表排序样式标识"),
            field("goodsScore", GoodsFieldType.FLOAT, "float", "商品评分"),
            field("showScore", GoodsFieldType.FLOAT, "float", "商品评分"),
            field("reviewScore", GoodsFieldType.STRING, "string", "商品综合评价得分"),
            field("hiddenComment", GoodsFieldType.BOOLEAN, "boolean", "是否隐藏评论"),
            field("seoLinkUrl", GoodsFieldType.STRING, "string", "seo链接"),
            field("defaultSelectSpec", GoodsFieldType.JSON, "array[object]", "默认选择的规格"),
            field("minOnSalePrice", GoodsFieldType.INTEGER, "integer", "销售价格最低值(单位分)"),
            field("maxOnSalePrice", GoodsFieldType.INTEGER, "integer", "销售价格最高值(单位分)"),
            field("priceInfo", GoodsFieldType.JSON, "object", "价格信息"),
            field("salePriceRich", GoodsFieldType.JSON, "array[object]", "销售价格富文本信息"),
            field("minToMaxSalePriceRich", GoodsFieldType.JSON, "array[object]", "minToMaxSalePriceRich"),
            field("image", GoodsFieldType.JSON, "object", "图片信息"),
            field("brandCard", GoodsFieldType.JSON, "object", "品牌卡片"),
            field("guideFile", GoodsFieldType.JSON, "object", "指导书文件"),
            field("hdThumbUrl", GoodsFieldType.STRING, "string", "缩略图链接"),
            field("thumbUrl", GoodsFieldType.STRING, "string", "略缩图链接"),
            field("goodsImage", GoodsFieldType.STRING, "string", "商品图片链接"),
            field("imageUrl", GoodsFieldType.STRING, "string", "商品图片链接"),
            field("longThumbUrl", GoodsFieldType.STRING, "string", "商品图片链接"),
            field("searchKey", GoodsFieldType.STRING, "string", "用户原始搜索关键词"),
            field("queryReleScore", GoodsFieldType.FLOAT, "float", "queryReleScore"),
            field("goodRankList", GoodsFieldType.JSON, "array[string]", "goodRankList"),
            field("video", GoodsFieldType.JSON, "object", "商品视频信息"),
            field("visible", GoodsFieldType.BOOLEAN, "boolean", "商品是否可见"),
            field("pageAlt", GoodsFieldType.STRING, "string", "商品页面描述信息"),
            field("itemType", GoodsFieldType.INTEGER, "integer", "itemType"),
            field("HWRatio", GoodsFieldType.INTEGER, "integer", "HWRatio"),
            field("semiManaged", GoodsFieldType.BOOLEAN, "boolean", "是否半托管"),
            field("benefitText", GoodsFieldType.JSON, "object", "折扣文本信息"),
            field("soldQuantityPercent", GoodsFieldType.INTEGER, "integer", "销量占库存百分比(存疑)"),
            field("wareHouseType", GoodsFieldType.INTEGER, "integer", "仓库类型"),
            field("isLocalGoods", GoodsFieldType.INTEGER, "integer", "是否为本地商品"),
            field("itemId", GoodsFieldType.STRING, "string", "itemId"),
            field("adultGoods", GoodsFieldType.BOOLEAN, "boolean", "是否为成人用品"),
            field("isManualChangeSpec", GoodsFieldType.BOOLEAN, "boolean", "isManualChangeSpec"),
            field("topItemsSize", GoodsFieldType.INTEGER, "integer", "商品在店铺的top排行榜索引"),
            field("topItemsTitle", GoodsFieldType.STRING, "string", "商品在店铺的top排行榜标题"),
            field("topItemsTitleBrief", GoodsFieldType.STRING, "string", "商品在店铺的top排行榜标题的解释"),
            field("topItemsTag", GoodsFieldType.STRING, "string", "商品在店铺的top排行榜的标签"),
            field("subTopItemsTitleUserText", GoodsFieldType.STRING, "string", "商品在店铺的top排行榜相关文本"),
            field("topItemsType", GoodsFieldType.INTEGER, "integer", "商品所在店铺的top排行榜类型"),
            field("iterationSku", GoodsFieldType.JSON, "array[string]", "skuid列表"),
            field("clothFitReviewText", GoodsFieldType.STRING, "string", "衣服适配评论文本"),
            field("popupClothFitReviewText", GoodsFieldType.STRING, "string", "衣服适配评论文本"),
            field("clothFitReviewInfoList", GoodsFieldType.JSON, "array[object]", "衣服适配的评论信息列表"),
            field("bestSellerModule", GoodsFieldType.JSON, "object", "畅销信息模块"),
            field("pageLabelsInfo", GoodsFieldType.JSON, "map", "商品评论标签信息"),
            field("gallery", GoodsFieldType.JSON, "array[object]", "商品轮播图列表"),
            field("goodsProperty", GoodsFieldType.JSON, "array[object]", "商品属性"),
            field("goodsPropertyPrefer", GoodsFieldType.JSON, "array[?]", "goodsPropertyPrefer"),
            field("bannerList", GoodsFieldType.JSON, "array[object]", "bannerList"),
            field("detailList", GoodsFieldType.JSON, "array[?]", "detailList"),
            field("customImageList", GoodsFieldType.JSON, "array[?]", "customImageList"),
            field("rows", GoodsFieldType.INTEGER, "integer", "rows"),
            field("statusExplain", GoodsFieldType.STRING, "string", "statusExplain"),
            field("checkQuantity", GoodsFieldType.INTEGER, "integer", "checkQuantity"),
            field("sizeGuide", GoodsFieldType.JSON, "object", "尺码指南"),
            field("skcList", GoodsFieldType.JSON, "array[object]", "商品的skc信息列表"),
            field("specIds", GoodsFieldType.STRING, "string", "规格id列表"),
            field("skuList", GoodsFieldType.JSON, "array[object]", "sku信息列表"),
            field("sizeSpecModule", GoodsFieldType.JSON, "object", "尺码表信息"),
            field("specCustom", GoodsFieldType.JSON, "object", "规格相关的信息"),
            field("productDetailFlatList", GoodsFieldType.JSON, "array[object]", "商品详情图列表(展开)"),
            field("productDetail", GoodsFieldType.JSON, "object", "商品详情图信息"),
            field("ext", GoodsFieldType.JSON, "object", "ext"),
            field("extendFields", GoodsFieldType.JSON, "object", "extendFields"),
            field("activityInfo", GoodsFieldType.JSON, "object", "活动信息"),
            field("displayEndTime", GoodsFieldType.INTEGER, "integer", "展示结束时间"),
            field("displayEndTimePercent", GoodsFieldType.INTEGER, "integer", "displayEndTimePercent"),
            field("displayEndTimePercentImg", GoodsFieldType.STRING, "string", "displayEndTimePercentImg"),
            field("selectedSpecIds", GoodsFieldType.JSON, "array[integer]", "selectedSpecIds"),
            field("quickLook", GoodsFieldType.JSON, "object", "快速预览信息"),
            field("crumbOptList", GoodsFieldType.JSON, "array[object]", "类目信息列表"),
            field("tagsInfo", GoodsFieldType.JSON, "object", "商品标签信息"),
            field("tagCode", GoodsFieldType.INTEGER, "integer", "店铺权益标签"),
            field("pRec", GoodsFieldType.JSON, "object", "pRec"),
            field("userAgent", GoodsFieldType.STRING, "string", "浏览器userAgent头"),
            field("fromUrl", GoodsFieldType.STRING, "string", "数据来源url"),
            field("serverTime", GoodsFieldType.STRING, "string", "服务器时间戳"),
            field("timezone", GoodsFieldType.STRING, "string", "时区")
    ));

    private static final List<FieldDef> DWD_FIELDS = Collections.unmodifiableList(Arrays.asList(
            field("goodsId", GoodsFieldType.INTEGER, "integer", "商品id"),
            field("siteId", GoodsFieldType.INTEGER, "integer", "商品所在站点，全托管商品不区分站点，该字段直接为空，半托管商品区分站点，该字段不为空"),
            field("date", GoodsFieldType.DATE, "date", "商品信息统计日期"),
            field("lang", GoodsFieldType.STRING, "string", "语言"),
            field("selectedCurrency", GoodsFieldType.STRING, "string", "选择的货币单位"),
            field("mallId", GoodsFieldType.STRING, "string", "商品所属店铺id"),
            field("optId", GoodsFieldType.INTEGER, "integer", "前端类目id"),
            field("catId", GoodsFieldType.INTEGER, "integer", "商家后台类目id"),
            field("title", GoodsFieldType.STRING, "string", "商品标题"),
            field("linkUrl", GoodsFieldType.STRING, "string", "商品链接"),
            field("salesNum", GoodsFieldType.INTEGER, "integer", "销售数量"),
            field("isOnsale", GoodsFieldType.INTEGER, "integer", "是否在售"),
            field("isShowMarketPrice", GoodsFieldType.BOOLEAN, "boolean", "是否展示原价"),
            field("reviewNum", GoodsFieldType.INTEGER, "integer", "评论数量"),
            field("goodsScore", GoodsFieldType.FLOAT, "float", "商品评分"),
            field("minOnSalePrice", GoodsFieldType.INTEGER, "integer", "销售价格最低值(单位分)"),
            field("maxOnSalePrice", GoodsFieldType.INTEGER, "integer", "销售价格最高值(单位分)"),
            field("priceInfo", GoodsFieldType.JSON, "object", "价格信息"),
            field("image", GoodsFieldType.JSON, "object", "图片信息"),
            field("brandCard", GoodsFieldType.JSON, "object", "品牌卡片"),
            field("guideFile", GoodsFieldType.JSON, "object", "指导书文件"),
            field("imageUrl", GoodsFieldType.STRING, "string", "商品图片链接"),
            field("thumbUrl", GoodsFieldType.STRING, "string", "略缩图链接"),
            field("video", GoodsFieldType.JSON, "object", "商品视频信息"),
            field("visible", GoodsFieldType.BOOLEAN, "boolean", "商品是否可见"),
            field("pageAlt", GoodsFieldType.STRING, "string", "商品页面描述信息"),
            field("semiManaged", GoodsFieldType.BOOLEAN, "boolean", "是否半托管"),
            field("soldQuantityPercent", GoodsFieldType.INTEGER, "integer", "销量占库存百分比(存疑)"),
            field("wareHouseType", GoodsFieldType.INTEGER, "integer", "仓库类型"),
            field("isLocalGoods", GoodsFieldType.INTEGER, "integer", "是否为本地商品"),
            field("adultGoods", GoodsFieldType.BOOLEAN, "boolean", "是否为成人用品"),
            field("gallery", GoodsFieldType.JSON, "array[object]", "商品轮播图列表"),
            field("goodsProperty", GoodsFieldType.JSON, "array[object]", "商品属性"),
            field("sizeGuide", GoodsFieldType.JSON, "object", "尺码指南"),
            field("skcList", GoodsFieldType.JSON, "array[object]", "商品的skc信息列表"),
            field("specIds", GoodsFieldType.STRING, "string", "规格id列表"),
            field("skuList", GoodsFieldType.JSON, "array[object]", "sku信息列表"),
            field("trackingKey", GoodsFieldType.STRING, "string", "关键词"),
            field("type", GoodsFieldType.INTEGER, "integer", "类型"),
            field("supportPromotion", GoodsFieldType.BOOLEAN, "boolean", "是否支持促销"),
            field("sizeSpecModule", GoodsFieldType.JSON, "object", "尺码表信息"),
            field("specCustom", GoodsFieldType.JSON, "object", "规格相关的信息"),
            field("productDetailFlatList", GoodsFieldType.JSON, "array[object]", "商品详情图列表(展开)"),
            field("productDetail", GoodsFieldType.JSON, "object", "商品详情图信息"),
            field("displayEndTime", GoodsFieldType.INTEGER, "integer", "展示结束时间"),
            field("crumbOptList", GoodsFieldType.JSON, "array[object]", "类目信息列表")
    ));

    private static final List<FieldDef> DWS_FIELDS = Collections.unmodifiableList(Arrays.asList(
            field("goodsId", GoodsFieldType.INTEGER, "integer", "商品id"),
            field("siteId", GoodsFieldType.INTEGER, "integer", "商品所在站点"),
            field("date", GoodsFieldType.DATE, "date", "商品信息统计日期"),
            field("goodsTitleEnglish", GoodsFieldType.STRING, "string", "商品英文标题"),
            field("goodsTitleChinese", GoodsFieldType.STRING, "string", "商品中文标题"),
            field("mallId", GoodsFieldType.STRING, "string", "商品所属店铺id"),
            field("mainImageUrl", GoodsFieldType.STRING, "string", "商品主图"),
            field("thumbUrl", GoodsFieldType.STRING, "string", "略缩图链接"),
            field("carouselImageUrls", GoodsFieldType.JSON, "array[string]", "商品轮播图列表"),
            field("optId", GoodsFieldType.INTEGER, "integer", "商品的temu前端的类目id"),
            field("catId", GoodsFieldType.INTEGER, "integer", "商品的temu后端的类目id"),
            field("isAdultGoods", GoodsFieldType.BOOLEAN, "boolean", "是否为成人用品"),
            field("videoInfo", GoodsFieldType.JSON, "object", "商品视频信息"),
            field("propertyList", GoodsFieldType.JSON, "array[object]", "属性列表"),
            field("salesNum", GoodsFieldType.INTEGER, "integer", "当前销售数量"),
            field("currency", GoodsFieldType.STRING, "string", "商品售卖货币类型"),
            field("minOnSalePrice", GoodsFieldType.INTEGER, "integer", "商品当前售卖规格最低价格"),
            field("maxOnSalePrice", GoodsFieldType.INTEGER, "integer", "商品当前售卖规格最高价格"),
            field("score", GoodsFieldType.FLOAT, "float", "商品当前评分"),
            field("reviewNum", GoodsFieldType.INTEGER, "integer", "商品当前评论数"),
            field("stockStatus", GoodsFieldType.INTEGER, "integer", "商品当前库存状态"),
            field("stockQuantity", GoodsFieldType.INTEGER, "integer", "商品当前库存数量"),
            field("addToRegionTime", GoodsFieldType.DATE, "date", "在该站点上架时间"),
            field("skuInfoList", GoodsFieldType.JSON, "array[object]", "sku信息"),
            field("firstAddTime", GoodsFieldType.DATE, "date", "该商品数据第一次被添加时间"),
            field("lastUpdateTime", GoodsFieldType.DATE, "date", "该商品数据上一次更新时间")
    ));

    private static final Map<String, FieldDef> ODS_BY_NAME = byName(ODS_FIELDS);
    private static final Map<String, FieldDef> DWD_BY_NAME = byName(DWD_FIELDS);
    private static final Map<String, FieldDef> DWS_BY_NAME = byName(DWS_FIELDS);

    private GoodsSchema() {
    }

    public static List<FieldDef> odsFields() {
        return ODS_FIELDS;
    }

    public static List<FieldDef> dwdFields() {
        return DWD_FIELDS;
    }

    public static List<FieldDef> dwsFields() {
        return DWS_FIELDS;
    }

    public static FieldDef odsField(String name) {
        return ODS_BY_NAME.get(normalizeFieldName(name));
    }

    public static FieldDef dwdField(String name) {
        return DWD_BY_NAME.get(normalizeFieldName(name));
    }

    public static FieldDef dwsField(String name) {
        return DWS_BY_NAME.get(normalizeFieldName(name));
    }

    public static boolean hasField(List<FieldDef> fields, String name) {
        String normalized = normalizeFieldName(name);
        for (FieldDef field : fields) {
            if (field.getName().equals(normalized)) {
                return true;
            }
        }
        return false;
    }

    public static String normalizeFieldName(String name) {
        if (name == null) {
            return null;
        }
        return name.trim().replaceAll("\\[\\]$", "").replaceAll("\\{\\}$", "");
    }

    private static FieldDef field(String name, GoodsFieldType type, String sourceType, String comment) {
        return new FieldDef(name, type, sourceType, comment);
    }

    private static Map<String, FieldDef> byName(List<FieldDef> fields) {
        Map<String, FieldDef> result = new LinkedHashMap<String, FieldDef>();
        for (FieldDef field : fields) {
            result.put(field.getName(), field);
        }
        return Collections.unmodifiableMap(result);
    }

    public static final class FieldDef {
        private final String name;
        private final GoodsFieldType type;
        private final String sourceType;
        private final String comment;

        private FieldDef(String name, GoodsFieldType type, String sourceType, String comment) {
            this.name = name;
            this.type = type;
            this.sourceType = sourceType;
            this.comment = comment;
        }

        public String getName() { return name; }
        public GoodsFieldType getType() { return type; }
        public String getSqlType() { return type.getSqlType(); }
        public String getSourceType() { return sourceType; }
        public String getComment() { return comment; }
    }
}
