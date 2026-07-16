package com.lingyun.business.common.sink;

import org.apache.doris.flink.sink.DorisAbstractCommittable;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.writer.DorisAbstractWriter;
import org.apache.doris.flink.sink.writer.DorisWriterState;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DorisWriteSuccessLogTest {

    @Test
    public void formatsDorisDdlCompositeKeys() {
        assertEquals("数据写入成功：[ods][goodsId=123,siteId=100,date=2026-05-27]",
                DorisWriteSuccessLogEntry.fromJson("ods", "ods_goods_raw",
                        "{\"goodsId\":123,\"siteId\":100,\"date\":\"2026-05-27\"}").formatMessage());
        assertEquals("数据写入成功：[dwd][goodsId=123,siteId=100,date=2026-05-27]",
                DorisWriteSuccessLogEntry.fromJson("dwd", "dwd_goods_base",
                        "{\"goodsId\":123,\"siteId\":100,\"date\":\"2026-05-27\"}").formatMessage());
        assertEquals("数据写入成功：[ods][date=2026-05-27,optId=cat-1,siteId=100]",
                DorisWriteSuccessLogEntry.fromJson("ods", "ods_category_raw",
                        "{\"date\":\"2026-05-27\",\"optId\":\"cat-1\",\"siteId\":\"100\"}").formatMessage());
        assertEquals("数据写入成功：[dwd][date=2026-05-27,optId=cat-1,siteId=100]",
                DorisWriteSuccessLogEntry.fromJson("dwd", "dwd_category_base",
                        "{\"date\":\"2026-05-27\",\"optId\":\"cat-1\",\"siteId\":\"100\"}").formatMessage());
        assertEquals("数据写入成功：[ods][date=2026-05-27,siteId=100]",
                DorisWriteSuccessLogEntry.fromJson("ods", "ods_site_raw",
                        "{\"date\":\"2026-05-27\",\"siteId\":\"100\"}").formatMessage());
        assertEquals("数据写入成功：[dwd][siteId=100,date=2026-05-27,regionId=US]",
                DorisWriteSuccessLogEntry.fromJson("dwd", "dwd_site_base",
                        "{\"siteId\":\"100\",\"date\":\"2026-05-27\",\"regionId\":\"US\"}").formatMessage());
        assertEquals("数据写入成功：[ods][mallId=mall-9,siteId=100,date=2026-05-27]",
                DorisWriteSuccessLogEntry.fromJson("ods", "ods_malls_raw",
                        "{\"mallId\":\"mall-9\",\"siteId\":\"100\",\"date\":\"2026-05-27\"}").formatMessage());
        assertEquals("数据写入成功：[dwd][mallId=mall-9,siteId=100,date=2026-05-27]",
                DorisWriteSuccessLogEntry.fromJson("dwd", "dwd_malls_base",
                        "{\"mallId\":\"mall-9\",\"siteId\":\"100\",\"date\":\"2026-05-27\"}").formatMessage());
        assertEquals("数据写入成功：[ods][date=2026-05-27,reviewId=rv-8,siteId=100]",
                DorisWriteSuccessLogEntry.fromJson("ods", "ods_review_raw",
                        "{\"date\":\"2026-05-27\",\"reviewId\":\"rv-8\",\"siteId\":100}").formatMessage());
        assertEquals("数据写入成功：[dwd][reviewId=rv-8,siteId=100,date=2026-05-27]",
                DorisWriteSuccessLogEntry.fromJson("dwd", "dwd_review_base",
                        "{\"date\":\"2026-05-27\",\"reviewId\":\"rv-8\",\"siteId\":100}").formatMessage());
    }

    @Test
    public void formatsMissingCompositeKeyPartsAsNull() {
        assertEquals("数据写入成功：[dwd][goodsId=null,siteId=null,date=null]",
                DorisWriteSuccessLogEntry.fromJson("dwd", "dwd_goods_base", "{}").formatMessage());
    }

    @Test
    public void serializesWrappedCommittableWithLogEntries() throws Exception {
        DorisCommittable delegate = new DorisCommittable("be:8040", "lingyun", 42L);
        DorisWriteSuccessLogEntry first = DorisWriteSuccessLogEntry.fromJson(
                "ods", "ods_goods_raw", "{\"goodsId\":123,\"siteId\":100,\"date\":\"2026-05-27\"}");
        DorisWriteSuccessLogEntry second = DorisWriteSuccessLogEntry.fromJson(
                "dwd", "dwd_review_base", "{\"reviewId\":\"rv-1\",\"siteId\":100,\"date\":\"2026-05-27\"}");
        DorisWriteSuccessCommittable wrapped = new DorisWriteSuccessCommittable(delegate, Arrays.asList(first, second));

        DorisWriteSuccessCommittableSerializer serializer =
                new DorisWriteSuccessCommittableSerializer(new FakeCommittableSerializer());

        DorisWriteSuccessCommittable restored = (DorisWriteSuccessCommittable) serializer.deserialize(
                serializer.getVersion(), serializer.serialize(wrapped));

        DorisCommittable restoredDelegate = (DorisCommittable) restored.getDelegate();
        assertEquals("be:8040", restoredDelegate.getHostPort());
        assertEquals("lingyun", restoredDelegate.getDb());
        assertEquals(42L, restoredDelegate.getTxnID());
        assertEquals("数据写入成功：[ods][goodsId=123,siteId=100,date=2026-05-27]",
                restored.getLogEntries().get(0).formatMessage());
        assertEquals("数据写入成功：[dwd][reviewId=rv-1,siteId=100,date=2026-05-27]",
                restored.getLogEntries().get(1).formatMessage());
    }

    @Test
    public void logsOnlyAfterDelegateCommitSucceeds() throws Exception {
        DorisWriteSuccessLogEntry entry = DorisWriteSuccessLogEntry.fromJson(
                "ods", "ods_goods_raw", "{\"goodsId\":123,\"siteId\":100,\"date\":\"2026-05-27\"}");
        DorisWriteSuccessCommittable wrapped = new DorisWriteSuccessCommittable(
                new DorisCommittable("be:8040", "lingyun", 42L), Collections.singletonList(entry));
        FakeCommitRequest request = new FakeCommitRequest(wrapped);
        List<DorisWriteSuccessLogEntry> logged = new ArrayList<>();

        DorisWriteSuccessCommitter committer = new DorisWriteSuccessCommitter(
                fakeCommitter(requests -> assertTrue(requests.iterator().next().getCommittable() instanceof DorisCommittable)),
                logged::add);

        committer.commit(Collections.singletonList(request));

        assertEquals(1, logged.size());
        assertEquals("数据写入成功：[ods][goodsId=123,siteId=100,date=2026-05-27]",
                logged.get(0).formatMessage());
        assertFalse(request.retryLater);
    }

    @Test
    public void doesNotLogWhenDelegateRequestsRetry() throws Exception {
        DorisWriteSuccessLogEntry entry = DorisWriteSuccessLogEntry.fromJson(
                "ods", "ods_goods_raw", "{\"goodsId\":123,\"siteId\":100,\"date\":\"2026-05-27\"}");
        DorisWriteSuccessCommittable wrapped = new DorisWriteSuccessCommittable(
                new DorisCommittable("be:8040", "lingyun", 42L), Collections.singletonList(entry));
        FakeCommitRequest request = new FakeCommitRequest(wrapped);
        List<DorisWriteSuccessLogEntry> logged = new ArrayList<>();

        DorisWriteSuccessCommitter committer = new DorisWriteSuccessCommitter(
                fakeCommitter(requests -> requests.iterator().next().retryLater()),
                logged::add);

        committer.commit(Collections.singletonList(request));

        assertTrue(logged.isEmpty());
        assertTrue(request.retryLater);
    }

    @Test
    public void logsDirectStreamLoadAfterPrepareCommitReturnsNoCommittables() throws Exception {
        FakeWriter writer = new FakeWriter(Collections.emptyList());
        List<DorisWriteSuccessLogEntry> logged = new ArrayList<>();
        DorisWriteSuccessWriter successWriter = new DorisWriteSuccessWriter(
                writer, "dwd", "dwd_category_base", logged::add);

        successWriter.write("{\"date\":\"2026-05-29\",\"optId\":31,\"siteId\":100}", null);
        Collection<DorisAbstractCommittable> committables = successWriter.prepareCommit();

        assertTrue(committables.isEmpty());
        assertEquals(1, logged.size());
        assertEquals("数据写入成功：[dwd][date=2026-05-29,optId=31,siteId=100]",
                logged.get(0).formatMessage());
    }

    private static class FakeCommittableSerializer implements SimpleVersionedSerializer<DorisAbstractCommittable> {
        @Override
        public int getVersion() {
            return 7;
        }

        @Override
        public byte[] serialize(DorisAbstractCommittable committable) throws IOException {
            DorisCommittable dorisCommittable = (DorisCommittable) committable;
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeUTF(dorisCommittable.getHostPort());
            out.writeUTF(dorisCommittable.getDb());
            out.writeLong(dorisCommittable.getTxnID());
            out.flush();
            return bytes.toByteArray();
        }

        @Override
        public DorisAbstractCommittable deserialize(int version, byte[] serialized) throws IOException {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));
            return new DorisCommittable(in.readUTF(), in.readUTF(), in.readLong());
        }
    }

    private static class FakeCommitRequest implements Committer.CommitRequest<DorisAbstractCommittable> {
        private DorisAbstractCommittable committable;
        private boolean retryLater;

        private FakeCommitRequest(DorisAbstractCommittable committable) {
            this.committable = committable;
        }

        @Override
        public DorisAbstractCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable throwable) {
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable throwable) {
        }

        @Override
        public void retryLater() {
            retryLater = true;
        }

        @Override
        public void updateAndRetryLater(DorisAbstractCommittable committable) {
            this.committable = committable;
            retryLater = true;
        }

        @Override
        public void signalAlreadyCommitted() {
        }
    }

    private static Committer<DorisAbstractCommittable> fakeCommitter(DelegateCommitter delegate) {
        return new Committer<DorisAbstractCommittable>() {
            @Override
            public void commit(Collection<CommitRequest<DorisAbstractCommittable>> requests)
                    throws IOException, InterruptedException {
                delegate.commit(requests);
            }

            @Override
            public void close() {
            }
        };
    }

    private interface DelegateCommitter {
        void commit(Collection<Committer.CommitRequest<DorisAbstractCommittable>> requests)
                throws IOException, InterruptedException;
    }

    private static class FakeWriter implements DorisAbstractWriter<String, DorisWriterState, DorisAbstractCommittable> {
        private final Collection<DorisAbstractCommittable> committables;

        private FakeWriter(Collection<DorisAbstractCommittable> committables) {
            this.committables = committables;
        }

        @Override
        public void write(String element, SinkWriter.Context context) {
        }

        @Override
        public void flush(boolean endOfInput) {
        }

        @Override
        public Collection<DorisAbstractCommittable> prepareCommit() {
            return committables;
        }

        @Override
        public List<DorisWriterState> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public void writeWatermark(Watermark watermark) {
        }

        @Override
        public void close() {
        }
    }
}
