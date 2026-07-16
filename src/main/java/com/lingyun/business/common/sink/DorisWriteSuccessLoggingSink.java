package com.lingyun.business.common.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisAbstractCommittable;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.DorisAbstractWriter;
import org.apache.doris.flink.sink.writer.DorisWriterState;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

class DorisWriteSuccessLoggingSink extends DorisSink<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DorisWriteSuccessLoggingSink.class);
    private static final String DORIS_BATCH_WRITER_CLASS = "org.apache.doris.flink.sink.batch.DorisBatchWriter";

    private final String layer;
    private final String tableName;

    DorisWriteSuccessLoggingSink(DorisOptions dorisOptions,
                                 DorisReadOptions dorisReadOptions,
                                 DorisExecutionOptions dorisExecutionOptions,
                                 DorisRecordSerializer<String> serializer,
                                 String layer,
                                 String tableName) {
        super(dorisOptions, dorisReadOptions, dorisExecutionOptions, serializer);
        this.layer = layer;
        this.tableName = tableName;
    }

    @Override
    public DorisAbstractWriter createWriter(Sink.InitContext initContext) throws IOException {
        return new DorisWriteSuccessWriter(
                super.createWriter(initContext),
                layer,
                tableName,
                entry -> LOGGER.info(entry.formatMessage()));
    }

    @Override
    public DorisAbstractWriter restoreWriter(Sink.InitContext initContext,
                                             Collection<DorisWriterState> recoveredState) throws IOException {
        return new DorisWriteSuccessWriter(
                super.restoreWriter(initContext, recoveredState),
                layer,
                tableName,
                entry -> LOGGER.info(entry.formatMessage()));
    }

    @Override
    public Committer createCommitter() throws IOException {
        return new DorisWriteSuccessCommitter(super.createCommitter(), entry -> LOGGER.info(entry.formatMessage()));
    }

    @Override
    public SimpleVersionedSerializer getCommittableSerializer() {
        return new DorisWriteSuccessCommittableSerializer(super.getCommittableSerializer());
    }

    static boolean isBatchWriter(DorisAbstractWriter delegate) {
        return delegate != null && DORIS_BATCH_WRITER_CLASS.equals(delegate.getClass().getName());
    }
}

class DorisWriteSuccessWriter implements DorisAbstractWriter<String, DorisWriterState, DorisAbstractCommittable> {
    private final DorisAbstractWriter<String, DorisWriterState, DorisAbstractCommittable> delegate;
    private final String layer;
    private final String tableName;
    private final Consumer<DorisWriteSuccessLogEntry> logConsumer;
    private final boolean flushConfirmsWrites;
    private final List<DorisWriteSuccessLogEntry> pendingLogEntries = new ArrayList<>();

    @SuppressWarnings("unchecked")
    DorisWriteSuccessWriter(DorisAbstractWriter delegate,
                            String layer,
                            String tableName,
                            Consumer<DorisWriteSuccessLogEntry> logConsumer) {
        this.delegate = (DorisAbstractWriter<String, DorisWriterState, DorisAbstractCommittable>) delegate;
        this.layer = layer;
        this.tableName = tableName;
        this.logConsumer = logConsumer;
        this.flushConfirmsWrites = DorisWriteSuccessLoggingSink.isBatchWriter(delegate);
    }

    @Override
    public void write(String element, SinkWriter.Context context) throws IOException, InterruptedException {
        delegate.write(element, context);
        pendingLogEntries.add(DorisWriteSuccessLogEntry.fromJson(layer, tableName, element));
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        delegate.flush(endOfInput);
        if (flushConfirmsWrites) {
            logPendingEntries();
        }
    }

    @Override
    public Collection<DorisAbstractCommittable> prepareCommit() throws IOException, InterruptedException {
        Collection<DorisAbstractCommittable> committables = delegate.prepareCommit();
        if (committables == null || committables.isEmpty()) {
            if (!flushConfirmsWrites) {
                logPendingEntries();
            }
            return committables;
        }

        List<DorisWriteSuccessLogEntry> entries = new ArrayList<>(pendingLogEntries);
        pendingLogEntries.clear();

        List<DorisAbstractCommittable> wrappedCommittables = new ArrayList<>(committables.size());
        boolean attachEntries = true;
        for (DorisAbstractCommittable committable : committables) {
            wrappedCommittables.add(new DorisWriteSuccessCommittable(
                    committable,
                    attachEntries ? entries : Collections.emptyList()));
            attachEntries = false;
        }
        return wrappedCommittables;
    }

    @Override
    public List<DorisWriterState> snapshotState(long checkpointId) throws IOException {
        return delegate.snapshotState(checkpointId);
    }

    @Override
    public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
        delegate.writeWatermark(watermark);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    private void logPendingEntries() {
        for (DorisWriteSuccessLogEntry entry : pendingLogEntries) {
            logConsumer.accept(entry);
        }
        pendingLogEntries.clear();
    }
}

class DorisWriteSuccessCommitter implements Committer<DorisAbstractCommittable> {
    private final Committer<DorisAbstractCommittable> delegate;
    private final Consumer<DorisWriteSuccessLogEntry> logConsumer;

    @SuppressWarnings("unchecked")
    DorisWriteSuccessCommitter(Committer delegate, Consumer<DorisWriteSuccessLogEntry> logConsumer) {
        this.delegate = (Committer<DorisAbstractCommittable>) delegate;
        this.logConsumer = logConsumer;
    }

    @Override
    public void commit(Collection<CommitRequest<DorisAbstractCommittable>> requests)
            throws IOException, InterruptedException {
        List<CommitRequest<DorisAbstractCommittable>> delegateRequests = new ArrayList<>(requests.size());
        List<DorisWriteSuccessCommitRequest> trackedRequests = new ArrayList<>();

        for (CommitRequest<DorisAbstractCommittable> request : requests) {
            DorisAbstractCommittable committable = request.getCommittable();
            if (committable instanceof DorisWriteSuccessCommittable) {
                DorisWriteSuccessCommitRequest trackedRequest = new DorisWriteSuccessCommitRequest(
                        request, (DorisWriteSuccessCommittable) committable);
                delegateRequests.add(trackedRequest);
                trackedRequests.add(trackedRequest);
            } else {
                delegateRequests.add(request);
            }
        }

        delegate.commit(delegateRequests);

        for (DorisWriteSuccessCommitRequest request : trackedRequests) {
            if (request.shouldLog()) {
                for (DorisWriteSuccessLogEntry entry : request.getLogEntries()) {
                    logConsumer.accept(entry);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}

class DorisWriteSuccessCommitRequest implements Committer.CommitRequest<DorisAbstractCommittable> {
    private final Committer.CommitRequest<DorisAbstractCommittable> delegate;
    private final DorisWriteSuccessCommittable originalCommittable;
    private boolean retryOrFailed;

    DorisWriteSuccessCommitRequest(Committer.CommitRequest<DorisAbstractCommittable> delegate,
                                   DorisWriteSuccessCommittable originalCommittable) {
        this.delegate = delegate;
        this.originalCommittable = originalCommittable;
    }

    @Override
    public DorisAbstractCommittable getCommittable() {
        return originalCommittable.getDelegate();
    }

    @Override
    public int getNumberOfRetries() {
        return delegate.getNumberOfRetries();
    }

    @Override
    public void signalFailedWithKnownReason(Throwable throwable) {
        retryOrFailed = true;
        delegate.signalFailedWithKnownReason(throwable);
    }

    @Override
    public void signalFailedWithUnknownReason(Throwable throwable) {
        retryOrFailed = true;
        delegate.signalFailedWithUnknownReason(throwable);
    }

    @Override
    public void retryLater() {
        retryOrFailed = true;
        delegate.retryLater();
    }

    @Override
    public void updateAndRetryLater(DorisAbstractCommittable committable) {
        retryOrFailed = true;
        delegate.updateAndRetryLater(new DorisWriteSuccessCommittable(
                unwrap(committable), originalCommittable.getLogEntries()));
    }

    @Override
    public void signalAlreadyCommitted() {
        delegate.signalAlreadyCommitted();
    }

    boolean shouldLog() {
        return !retryOrFailed;
    }

    List<DorisWriteSuccessLogEntry> getLogEntries() {
        return originalCommittable.getLogEntries();
    }

    private static DorisAbstractCommittable unwrap(DorisAbstractCommittable committable) {
        if (committable instanceof DorisWriteSuccessCommittable) {
            return ((DorisWriteSuccessCommittable) committable).getDelegate();
        }
        return committable;
    }
}

class DorisWriteSuccessCommittable implements DorisAbstractCommittable {
    private final DorisAbstractCommittable delegate;
    private final List<DorisWriteSuccessLogEntry> logEntries;

    DorisWriteSuccessCommittable(DorisAbstractCommittable delegate, List<DorisWriteSuccessLogEntry> logEntries) {
        this.delegate = delegate;
        this.logEntries = Collections.unmodifiableList(new ArrayList<>(logEntries));
    }

    DorisAbstractCommittable getDelegate() {
        return delegate;
    }

    List<DorisWriteSuccessLogEntry> getLogEntries() {
        return logEntries;
    }
}

class DorisWriteSuccessCommittableSerializer implements SimpleVersionedSerializer<DorisAbstractCommittable> {
    private static final int VERSION = 1002;
    private static final int SINGLE_KEY_VERSION = 1001;

    private final SimpleVersionedSerializer<DorisAbstractCommittable> delegateSerializer;

    @SuppressWarnings("unchecked")
    DorisWriteSuccessCommittableSerializer(SimpleVersionedSerializer delegateSerializer) {
        this.delegateSerializer = (SimpleVersionedSerializer<DorisAbstractCommittable>) delegateSerializer;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DorisAbstractCommittable committable) throws IOException {
        DorisWriteSuccessCommittable wrapped = wrap(committable);
        byte[] delegateBytes = delegateSerializer.serialize(wrapped.getDelegate());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(delegateSerializer.getVersion());
        out.writeInt(delegateBytes.length);
        out.write(delegateBytes);
        out.writeInt(wrapped.getLogEntries().size());
        for (DorisWriteSuccessLogEntry entry : wrapped.getLogEntries()) {
            writeNullableString(out, entry.getLayer());
            out.writeInt(entry.getKeyParts().size());
            for (DorisWriteSuccessKeyPart keyPart : entry.getKeyParts()) {
                writeNullableString(out, keyPart.getName());
                writeNullableString(out, keyPart.getValue());
            }
        }
        out.flush();
        return bytes.toByteArray();
    }

    @Override
    public DorisAbstractCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION && version != SINGLE_KEY_VERSION) {
            return new DorisWriteSuccessCommittable(
                    delegateSerializer.deserialize(version, serialized),
                    Collections.emptyList());
        }

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));
        int delegateVersion = in.readInt();
        int delegateBytesLength = in.readInt();
        byte[] delegateBytes = new byte[delegateBytesLength];
        in.readFully(delegateBytes);
        DorisAbstractCommittable delegate = delegateSerializer.deserialize(delegateVersion, delegateBytes);

        int logEntryCount = in.readInt();
        List<DorisWriteSuccessLogEntry> logEntries = new ArrayList<>(logEntryCount);
        for (int i = 0; i < logEntryCount; i++) {
            String layer = readNullableString(in);
            if (version == SINGLE_KEY_VERSION) {
                logEntries.add(new DorisWriteSuccessLogEntry(layer, Collections.singletonList(
                        new DorisWriteSuccessKeyPart(readNullableString(in), readNullableString(in)))));
            } else {
                int keyPartCount = in.readInt();
                List<DorisWriteSuccessKeyPart> keyParts = new ArrayList<>(keyPartCount);
                for (int j = 0; j < keyPartCount; j++) {
                    keyParts.add(new DorisWriteSuccessKeyPart(readNullableString(in), readNullableString(in)));
                }
                logEntries.add(new DorisWriteSuccessLogEntry(layer, keyParts));
            }
        }
        return new DorisWriteSuccessCommittable(delegate, logEntries);
    }

    private static DorisWriteSuccessCommittable wrap(DorisAbstractCommittable committable) {
        if (committable instanceof DorisWriteSuccessCommittable) {
            return (DorisWriteSuccessCommittable) committable;
        }
        return new DorisWriteSuccessCommittable(committable, Collections.emptyList());
    }

    private static void writeNullableString(DataOutputStream out, String value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            out.writeUTF(value);
        }
    }

    private static String readNullableString(DataInputStream in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }
        return in.readUTF();
    }
}

class DorisWriteSuccessLogEntry implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String layer;
    private final List<DorisWriteSuccessKeyPart> keyParts;

    DorisWriteSuccessLogEntry(String layer, List<DorisWriteSuccessKeyPart> keyParts) {
        this.layer = layer;
        this.keyParts = Collections.unmodifiableList(new ArrayList<>(keyParts));
    }

    static DorisWriteSuccessLogEntry fromJson(String layer, String tableName, String json) {
        List<String> keyNames = resolveKeyNames(tableName);
        JsonNode root = null;
        if (json != null && !json.isEmpty()) {
            try {
                root = OBJECT_MAPPER.readTree(json);
            } catch (Exception ignored) {
                root = null;
            }
        }

        List<DorisWriteSuccessKeyPart> keyParts = new ArrayList<>(keyNames.size());
        for (String keyName : keyNames) {
            keyParts.add(new DorisWriteSuccessKeyPart(
                    keyName,
                    root == null ? null : nodeToString(root.get(keyName))));
        }
        return new DorisWriteSuccessLogEntry(layer, keyParts);
    }

    String formatMessage() {
        return String.format("数据写入成功：[%s][%s]", layer, formatKeyParts());
    }

    String getLayer() {
        return layer;
    }

    List<DorisWriteSuccessKeyPart> getKeyParts() {
        return keyParts;
    }

    private String formatKeyParts() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < keyParts.size(); i++) {
            if (i > 0) {
                builder.append(',');
            }
            DorisWriteSuccessKeyPart keyPart = keyParts.get(i);
            builder.append(keyPart.getName()).append('=').append(keyPart.getValue());
        }
        return builder.toString();
    }

    private static List<String> resolveKeyNames(String tableName) {
        if (tableName == null) {
            return Collections.singletonList("id");
        }
        if ("ods_goods_raw".equals(tableName) || "dwd_goods_base".equals(tableName)) {
            return Arrays.asList("goodsId", "siteId", "date");
        }
        if ("ods_geekbi_goods_raw".equals(tableName) || "dwd_geekbi_goods_daily".equals(tableName)) {
            return Arrays.asList("goodsId", "siteId", "date");
        }
        if ("ods_category_raw".equals(tableName) || "dwd_category_base".equals(tableName)) {
            return Arrays.asList("date", "optId", "siteId");
        }
        if ("ods_site_raw".equals(tableName)) {
            return Arrays.asList("date", "siteId");
        }
        if ("dwd_site_base".equals(tableName)) {
            return Arrays.asList("siteId", "date", "regionId");
        }
        if ("ods_malls_raw".equals(tableName) || "dwd_malls_base".equals(tableName)) {
            return Arrays.asList("mallId", "siteId", "date");
        }
        if ("ods_review_raw".equals(tableName)) {
            return Arrays.asList("date", "reviewId", "siteId");
        }
        if ("dwd_review_base".equals(tableName)) {
            return Arrays.asList("reviewId", "siteId", "date");
        }
        return Collections.singletonList("id");
    }

    private static String nodeToString(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isValueNode()) {
            return node.asText();
        }
        return node.toString();
    }
}

class DorisWriteSuccessKeyPart implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final String value;

    DorisWriteSuccessKeyPart(String name, String value) {
        this.name = name;
        this.value = value;
    }

    String getName() {
        return name;
    }

    String getValue() {
        return value;
    }
}
