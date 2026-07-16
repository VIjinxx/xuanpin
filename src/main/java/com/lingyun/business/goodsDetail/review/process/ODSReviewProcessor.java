package com.lingyun.business.goodsDetail.review.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lingyun.business.common.config.FieldMappingLoader;
import com.lingyun.business.common.model.goods.GoodsSchemaConverter;
import com.lingyun.business.common.model.review.ODSReviewRecord;
import com.lingyun.business.common.util.JsonUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ODS层评论数据处理器。
 * 将一条商品详情页数据按 reviewInfoList 展开为多条评论记录。
 */
public class ODSReviewProcessor extends RichFlatMapFunction<String, ODSReviewRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ODSReviewProcessor.class);
    private static final ObjectMapper OBJECT_MAPPER = JsonUtil.getObjectMapper();
    private static final Pattern BRACKET_ARRAY_INDEX_PATTERN = Pattern.compile("\\[([0-9]+|i)]");

    private static final String MAPPING_FILE =
            "mappingFile/goodsDetail/ods_goods_detail_review_mapping.json";
    private static final String DEFAULT_REVIEW_LIST_PATH = "review.reviewData.reviewInfoList";
    private static final int JSON_PREVIEW_LENGTH = 200;

    private static final Map<String, String[]> FIELD_ALIASES = createFieldAliases();

    private transient Map<String, List<String>> fieldMappings;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ensureMappingsLoaded();
        LOGGER.info("ODSReviewProcessor 初始化完成,映射配置: {}", MAPPING_FILE);
    }

    @Override
    public void close() throws Exception {
        this.fieldMappings = null;
        LOGGER.info("ODSReviewProcessor 资源已释放");
        super.close();
    }

    @Override
    public void flatMap(String jsonStr, Collector<ODSReviewRecord> out) throws Exception {
        if (jsonStr == null || jsonStr.trim().isEmpty()) {
            return;
        }

        ensureMappingsLoaded();
        JsonNode rootNode = JsonUtil.parseJson(jsonStr);
        if (rootNode == null) {
            LOGGER.warn("解析JSON失败,跳过评论处理: inputLength={}, preview={}",
                    jsonStr.length(), previewJson(jsonStr));
            return;
        }

        List<SourceContext> sourceContexts = createSourceContexts();
        List<String> sourceItemSummaries = new ArrayList<>();
        int totalReviewItems = 0;
        int emittedRecords = 0;
        int skippedMissingReviewId = 0;

        for (SourceContext sourceContext : sourceContexts) {
            JsonNode reviewListNode = resolveSourceRoot(rootNode, sourceContext.itemRootPath);
            List<JsonNode> reviewItems = toReviewItems(reviewListNode);
            sourceItemSummaries.add(sourceContext.sourceIndex + ":" + sourceContext.itemRootPath
                    + "=" + reviewItems.size());
            totalReviewItems += reviewItems.size();

            for (JsonNode reviewNode : reviewItems) {
                String reviewId = resolveReviewId(rootNode, reviewNode, sourceContext);
                if (!hasText(reviewId)) {
                    skippedMissingReviewId++;
                    continue;
                }

                out.collect(buildRecord(rootNode, reviewNode, sourceContext, reviewId));
                emittedRecords++;
            }
        }

        if (emittedRecords == 0) {
            LOGGER.warn("ODS评论解析结果为空: sources={}, reviewItems={}, skippedMissingReviewId={}, sourceItems={}, inputLength={}",
                    sourceContexts.size(), totalReviewItems, skippedMissingReviewId,
                    String.join(";", sourceItemSummaries), jsonStr.length());
        } else {
            LOGGER.info("ODS评论解析完成: sources={}, reviewItems={}, emitted={}, skippedMissingReviewId={}, sourceItems={}",
                    sourceContexts.size(), totalReviewItems, emittedRecords, skippedMissingReviewId,
                    String.join(";", sourceItemSummaries));
        }
    }

    private void ensureMappingsLoaded() {
        if (fieldMappings == null) {
            fieldMappings = FieldMappingLoader.getFieldMappingSources(MAPPING_FILE);
        }
    }

    private List<SourceContext> createSourceContexts() {
        int sourceCount = getReviewSourceCount();
        List<SourceContext> contexts = new ArrayList<>();
        for (int sourceIndex = 0; sourceIndex < sourceCount; sourceIndex++) {
            String reviewIdPath = getSourcePath("reviewId", sourceIndex);
            if (reviewIdPath == null || reviewIdPath.isEmpty()) {
                if (sourceIndex == 0) {
                    reviewIdPath = DEFAULT_REVIEW_LIST_PATH + "[i].reviewId";
                } else {
                    continue;
                }
            }

            SourcePathInfo pathInfo = parseSourcePath(reviewIdPath);
            if (pathInfo != null) {
                contexts.add(new SourceContext(sourceIndex, pathInfo.itemRootPath));
            }
        }
        return contexts;
    }

    private int getReviewSourceCount() {
        List<String> reviewIdSources = fieldMappings.get("reviewId");
        if (reviewIdSources != null && !reviewIdSources.isEmpty()) {
            return reviewIdSources.size();
        }

        int maxSourceCount = 1;
        for (List<String> sources : fieldMappings.values()) {
            if (sources != null && !sources.isEmpty()) {
                maxSourceCount = Math.max(maxSourceCount, sources.size());
            }
        }
        return maxSourceCount;
    }

    private JsonNode resolveSourceRoot(JsonNode rootNode, String itemRootPath) {
        if (itemRootPath == null || itemRootPath.isEmpty()) {
            return rootNode;
        }
        return resolvePath(rootNode, itemRootPath, true);
    }

    private List<JsonNode> toReviewItems(JsonNode reviewListNode) {
        List<JsonNode> items = new ArrayList<>();
        collectReviewItems(reviewListNode, items);
        return items;
    }

    private void collectReviewItems(JsonNode node, List<JsonNode> items) {
        if (node == null || node.isNull()) {
            return;
        }
        if (node.isObject()) {
            items.add(node);
            return;
        }
        if (node.isArray()) {
            for (JsonNode item : node) {
                collectReviewItems(item, items);
            }
        }
    }

    private String resolveReviewId(JsonNode rootNode, JsonNode reviewNode, SourceContext sourceContext) {
        List<String> reviewIdSources = fieldMappings.get("reviewId");
        String mappingPath = getSourcePath(reviewIdSources, sourceContext.sourceIndex);
        JsonNode value = resolveFieldNode(rootNode, reviewNode, sourceContext,
                "reviewId", mappingPath, reviewIdSources);
        return value == null || value.isNull() ? null : normalizeReviewId(asString(value));
    }

    private ODSReviewRecord buildRecord(JsonNode rootNode, JsonNode reviewNode,
                                        SourceContext sourceContext, String reviewId) {
        ODSReviewRecord record = new ODSReviewRecord();
        record.setDate(LocalDate.now().toString());
        record.setReviewId(reviewId);

        for (Map.Entry<String, List<String>> entry : fieldMappings.entrySet()) {
            String fieldName = entry.getKey();
            if ("date".equals(fieldName) || "reviewId".equals(fieldName)) {
                continue;
            }

            String mappingPath = getSourcePath(entry.getValue(), sourceContext.sourceIndex);
            JsonNode value = resolveFieldNode(rootNode, reviewNode, sourceContext,
                    fieldName, mappingPath, entry.getValue());
            if (value == null || value.isNull()) {
                continue;
            }
            setField(record, fieldName, value);
        }

        return record;
    }

    private String normalizeReviewId(String reviewId) {
        return reviewId == null ? null : reviewId.trim();
    }

    private boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private String getSourcePath(String fieldName, int sourceIndex) {
        return getSourcePath(fieldMappings.get(fieldName), sourceIndex);
    }

    private String getSourcePath(List<String> sources, int sourceIndex) {
        if (sources == null || sources.isEmpty()) {
            return null;
        }
        if (sources.size() == 1) {
            return cleanSourcePath(sources.get(0));
        }
        if (sourceIndex < 0 || sourceIndex >= sources.size()) {
            return null;
        }
        return cleanSourcePath(sources.get(sourceIndex));
    }

    private String cleanSourcePath(String sourcePath) {
        return sourcePath == null ? "" : sourcePath.trim();
    }

    private JsonNode resolveFieldNode(JsonNode rootNode, JsonNode reviewNode, SourceContext sourceContext,
                                      String fieldName, String mappingPath, List<String> sources) {
        if (mappingPath == null || mappingPath.isEmpty()) {
            if (sources != null && sources.size() > 1) {
                return null;
            }
            return resolveReviewItemField(reviewNode, fieldName);
        }

        String relativeReviewPath = relativeReviewItemPath(mappingPath, sourceContext.itemRootPath);
        if (relativeReviewPath != null) {
            if (!relativeReviewPath.isEmpty()) {
                JsonNode mappedItemValue = resolvePath(reviewNode, relativeReviewPath, true);
                if (mappedItemValue != null && !mappedItemValue.isNull()) {
                    return mappedItemValue;
                }
            }
            return resolveReviewItemField(reviewNode, fieldName);
        }

        JsonNode rootValue = resolvePath(rootNode, mappingPath, true);
        if (rootValue != null && !rootValue.isNull()) {
            return rootValue;
        }

        if (sources != null && sources.size() == 1) {
            return resolveReviewItemField(reviewNode, fieldName);
        }

        return null;
    }

    private String relativeReviewItemPath(String path, String itemRootPath) {
        String dottedPath = toDottedArrayPath(path);
        String reviewListPath = toDottedArrayPath(itemRootPath);
        if (reviewListPath == null || reviewListPath.isEmpty()) {
            return dottedPath;
        }
        if (reviewListPath.equals(dottedPath)) {
            return "";
        }
        String prefix = reviewListPath + ".";
        if (!dottedPath.startsWith(prefix)) {
            return null;
        }

        String relativePath = dottedPath.substring(prefix.length());
        int dotIndex = relativePath.indexOf('.');
        if (dotIndex < 0) {
            return isArrayItemSegment(relativePath) ? "" : relativePath;
        }
        String firstSegment = relativePath.substring(0, dotIndex);
        return isArrayItemSegment(firstSegment) ? relativePath.substring(dotIndex + 1) : relativePath;
    }

    private SourcePathInfo parseSourcePath(String sourcePath) {
        String dottedPath = toDottedArrayPath(sourcePath);
        if (dottedPath == null || dottedPath.isEmpty()) {
            return null;
        }

        String[] segments = dottedPath.split("\\.");
        int arrayIndexSegment = -1;
        for (int i = segments.length - 2; i >= 0; i--) {
            if (isArrayItemSegment(segments[i])) {
                arrayIndexSegment = i;
                break;
            }
        }

        if (arrayIndexSegment >= 0) {
            return new SourcePathInfo(joinSegments(segments, 0, arrayIndexSegment));
        }

        return new SourcePathInfo(joinSegments(segments, 0, Math.max(segments.length - 1, 0)));
    }

    private String joinSegments(String[] segments, int startInclusive, int endExclusive) {
        StringBuilder builder = new StringBuilder();
        for (int i = startInclusive; i < endExclusive; i++) {
            if (segments[i].isEmpty()) {
                continue;
            }
            if (builder.length() > 0) {
                builder.append('.');
            }
            builder.append(segments[i]);
        }
        return builder.toString();
    }

    private JsonNode resolveReviewItemField(JsonNode reviewNode, String fieldName) {
        List<String> candidates = new ArrayList<>();
        candidates.add(fieldName);
        String[] aliases = FIELD_ALIASES.get(fieldName);
        if (aliases != null) {
            Collections.addAll(candidates, aliases);
        }

        for (String candidate : candidates) {
            JsonNode value = resolvePath(reviewNode, candidate, true);
            if (value != null && !value.isNull()) {
                return value;
            }
        }
        return null;
    }

    private void setField(ODSReviewRecord record, String fieldName, JsonNode value) {
        try {
            switch (fieldName) {
                case "reviewId":
                    record.setReviewId(asString(value));
                    break;
                case "siteId":
                    record.setSiteId(asLong(value));
                    break;
                case "goodsId":
                    record.setGoodsId(asString(value));
                    break;
                case "skuId":
                    record.setSkuId(asString(value));
                    break;
                case "comment":
                    record.setComment(asString(value));
                    break;
                case "score":
                    record.setScore(asLong(value));
                    break;
                case "specs":
                    record.setSpecs(value);
                    break;
                case "time":
                    record.setTime(asLong(value));
                    break;
                case "timeMs":
                    record.setTimeMs(asLong(value));
                    break;
                case "concatTimeLang":
                    record.setConcatTimeLang(asString(value));
                    break;
                case "concatRichText":
                    record.setConcatRichText(value);
                    break;
                case "avatar":
                    record.setAvatar(asString(value));
                    break;
                case "name":
                    record.setName(asString(value));
                    break;
                case "isOwnReview":
                    record.setIsOwnReview(asBoolean(value));
                    break;
                case "isSimilarReview":
                    record.setIsSimilarReview(asBoolean(value));
                    break;
                case "profileLinkUrl":
                    record.setProfileLinkUrl(asString(value));
                    break;
                case "pictures":
                    record.setPictures(value);
                    break;
                case "opList":
                    record.setOpList(value);
                    break;
                case "viewMoreList":
                    record.setViewMoreList(value);
                    break;
                case "reviewLang":
                    record.setReviewLang(value);
                    break;
                case "list":
                    record.setList(value);
                    break;
                case "goodsSpecificReviewLevelInfo":
                    record.setGoodsSpecificReviewLevelInfo(value);
                    break;
                case "extendParams":
                    record.setExtendParams(value);
                    break;
                case "inBlacklist":
                    record.setInBlacklist(asBoolean(value));
                    break;
                case "userAgent":
                    record.setUserAgent(asString(value));
                    break;
                case "fromUrl":
                    record.setFromUrl(asString(value));
                    break;
                case "serverTime":
                    record.setServerTime(asString(value));
                    break;
                case "timezone":
                    record.setTimezone(asString(value));
                    break;
                case "lang":
                    record.setLang(asString(value));
                    break;
                case "selectedLang":
                    record.setSelectedLang(asString(value));
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            LOGGER.debug("评论字段 {} 设置失败: {}", fieldName, e.getMessage());
        }
    }

    private String asString(JsonNode value) {
        return JsonUtil.nodeToString(value);
    }

    private Long asLong(JsonNode value) {
        return (Long) GoodsSchemaConverter.convertToParameterType(value, Long.class);
    }

    private Boolean asBoolean(JsonNode value) {
        return (Boolean) GoodsSchemaConverter.convertToParameterType(value, Boolean.class);
    }

    private JsonNode resolvePath(JsonNode node, String path, boolean allowFlattenedRebuild) {
        if (node == null || path == null || path.isEmpty()) {
            return null;
        }

        JsonNode directNode = getDirectField(node, path);
        if (directNode != null) {
            if (allowFlattenedRebuild) {
                JsonNode rebuilt = rebuildFromFlattenedFields(node, path);
                return rebuilt == null ? directNode : rebuilt;
            }
            return directNode;
        }

        String dottedPath = toDottedArrayPath(path);
        JsonNode current = node;
        String[] segments = dottedPath.split("\\.");
        for (String segment : segments) {
            if (segment.isEmpty() || current == null || current.isNull()) {
                return null;
            }

            JsonNode fullFlatNode = getDirectField(current, dottedPath);
            if (fullFlatNode != null) {
                return fullFlatNode;
            }

            if (current.isArray()) {
                Integer index = parseArrayIndex(segment);
                if (index == null || index < 0 || index >= current.size()) {
                    return allowFlattenedRebuild ? rebuildFromFlattenedFields(node, path) : null;
                }
                current = current.get(index);
            } else {
                if (current.isObject() && "value".equals(segment) && !current.has(segment)) {
                    current = collectObjectValues(current);
                } else if (!current.has(segment)) {
                    return allowFlattenedRebuild ? rebuildFromFlattenedFields(node, path) : null;
                } else {
                    current = current.get(segment);
                }
            }
        }
        return current == null || current.isNull() ? null : current;
    }

    private ArrayNode collectObjectValues(JsonNode objectNode) {
        ArrayNode values = OBJECT_MAPPER.createArrayNode();
        Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
        while (fields.hasNext()) {
            values.add(fields.next().getValue());
        }
        return values;
    }

    private JsonNode getDirectField(JsonNode node, String path) {
        if (node == null || !node.isObject() || path == null || path.isEmpty()) {
            return null;
        }
        if (node.has(path)) {
            return node.get(path);
        }
        String dottedPath = toDottedArrayPath(path);
        return node.has(dottedPath) ? node.get(dottedPath) : null;
    }

    private JsonNode rebuildFromFlattenedFields(JsonNode source, String path) {
        if (source == null || !source.isObject() || path == null || path.isEmpty()) {
            return null;
        }

        String dottedPath = toDottedArrayPath(path);
        String prefix = dottedPath + ".";
        ObjectNode rebuilt = OBJECT_MAPPER.createObjectNode();
        boolean found = false;

        Iterator<Map.Entry<String, JsonNode>> fields = source.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String dottedKey = toDottedArrayPath(entry.getKey());
            if (!dottedKey.startsWith(prefix)) {
                continue;
            }
            String relativePath = dottedKey.substring(prefix.length());
            if (relativePath.isEmpty()) {
                continue;
            }
            putFlattenedValue(rebuilt, relativePath, entry.getValue());
            found = true;
        }

        return found ? compactNumericObjects(rebuilt, true) : null;
    }

    private void putFlattenedValue(ObjectNode root, String relativePath, JsonNode value) {
        String[] segments = relativePath.split("\\.");
        ObjectNode current = root;
        for (int i = 0; i < segments.length; i++) {
            String segment = segments[i];
            if (segment.isEmpty()) {
                return;
            }
            if (i == segments.length - 1) {
                current.set(segment, value.deepCopy());
                return;
            }

            JsonNode next = current.get(segment);
            if (!(next instanceof ObjectNode)) {
                ObjectNode child = OBJECT_MAPPER.createObjectNode();
                current.set(segment, child);
                current = child;
            } else {
                current = (ObjectNode) next;
            }
        }
    }

    private JsonNode compactNumericObjects(JsonNode node, boolean topLevel) {
        if (node == null || node.isNull()) {
            return node;
        }
        if (node.isArray()) {
            ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
            for (JsonNode item : node) {
                arrayNode.add(compactNumericObjects(item, false));
            }
            return arrayNode;
        }
        if (!node.isObject()) {
            return node.deepCopy();
        }

        ObjectNode objectNode = (ObjectNode) node;
        TreeMap<Integer, JsonNode> numericChildren = new TreeMap<>();
        boolean hasField = false;
        boolean allNumeric = true;
        Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
        while (fields.hasNext()) {
            hasField = true;
            Map.Entry<String, JsonNode> entry = fields.next();
            Integer index = parseArrayIndex(entry.getKey());
            if (index == null) {
                allNumeric = false;
                break;
            }
            numericChildren.put(index, entry.getValue());
        }

        if (hasField && allNumeric && isZeroBasedContiguous(numericChildren)) {
            if (topLevel && numericChildren.size() == 1) {
                return compactNumericObjects(numericChildren.firstEntry().getValue(), false);
            }
            ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
            for (JsonNode child : numericChildren.values()) {
                arrayNode.add(compactNumericObjects(child, false));
            }
            return arrayNode;
        }

        ObjectNode compacted = OBJECT_MAPPER.createObjectNode();
        fields = objectNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            compacted.set(entry.getKey(), compactNumericObjects(entry.getValue(), false));
        }
        return compacted;
    }

    private boolean isZeroBasedContiguous(TreeMap<Integer, JsonNode> numericChildren) {
        int expected = 0;
        for (Integer index : numericChildren.keySet()) {
            if (index != expected) {
                return false;
            }
            expected++;
        }
        return !numericChildren.isEmpty();
    }

    private Integer parseArrayIndex(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return null;
            }
        }
        try {
            long index = Long.parseLong(value);
            return index > Integer.MAX_VALUE ? null : (int) index;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private boolean isArrayItemSegment(String value) {
        return "i".equals(value) || parseArrayIndex(value) != null;
    }

    private String toDottedArrayPath(String path) {
        Matcher matcher = BRACKET_ARRAY_INDEX_PATTERN.matcher(path);
        return matcher.replaceAll(".$1");
    }

    private String previewJson(String jsonStr) {
        String preview = jsonStr.replaceAll("\\s+", " ").trim();
        if (preview.length() <= JSON_PREVIEW_LENGTH) {
            return preview;
        }
        return preview.substring(0, JSON_PREVIEW_LENGTH) + "...";
    }

    private static class SourceContext {
        private final int sourceIndex;
        private final String itemRootPath;

        private SourceContext(int sourceIndex, String itemRootPath) {
            this.sourceIndex = sourceIndex;
            this.itemRootPath = itemRootPath;
        }
    }

    private static class SourcePathInfo {
        private final String itemRootPath;

        private SourcePathInfo(String itemRootPath) {
            this.itemRootPath = itemRootPath;
        }
    }

    private static Map<String, String[]> createFieldAliases() {
        Map<String, String[]> aliases = new HashMap<>();
        aliases.put("reviewId", new String[]{"review_id", "commentId", "comment_id", "id"});
        aliases.put("comment", new String[]{"content", "commentText", "text"});
        aliases.put("score", new String[]{"rating", "star", "starRating"});
        aliases.put("avatar", new String[]{"userAvatar", "user.avatar"});
        aliases.put("name", new String[]{"nickname", "nickName", "userName", "user.name"});
        aliases.put("pictures", new String[]{"pictureList", "picList", "pics"});
        aliases.put("list", new String[]{"imageOrVideoList", "mediaList"});
        aliases.put("profileLinkUrl", new String[]{"profileUrl", "profileLink", "user.profileLinkUrl"});
        aliases.put("reviewLang", new String[]{"langInfo", "languageInfo"});
        aliases.put("extendParams", new String[]{"extParams", "expParams"});
        return Collections.unmodifiableMap(new LinkedHashMap<>(aliases));
    }
}
