package com.lingyun.business.common.util;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JobSourceFactoryTest {

    @Test
    public void defaultsToKafkaWhenLocalIsMissing() {
        JobSourceFactory.SourceOptions options =
                JobSourceFactory.resolveOptions(new String[0], "samples/default.json");

        assertFalse(options.isLocal());
        assertFalse(options.hasFileOption());
        assertEquals("samples/default.json", options.getSourcePath());
    }

    @Test
    public void localUsesDefaultSampleWhenFileIsMissing() {
        JobSourceFactory.SourceOptions options =
                JobSourceFactory.resolveOptions(new String[] {"--local", "true"}, "samples/default.json");

        assertTrue(options.isLocal());
        assertFalse(options.hasFileOption());
        assertEquals("samples/default.json", options.getSourcePath());
    }

    @Test
    public void localFileOverridesDefaultSample() {
        JobSourceFactory.SourceOptions options =
                JobSourceFactory.resolveOptions(
                        new String[] {"--local", "true", "--file", "E:\\data\\demo.json"},
                        "samples/default.json");

        assertTrue(options.isLocal());
        assertTrue(options.hasFileOption());
        assertEquals("E:\\data\\demo.json", options.getSourcePath());
    }

    @Test
    public void localAlsoWorksWhenArgumentsArriveAsSingleString() {
        JobSourceFactory.SourceOptions options =
                JobSourceFactory.resolveOptions(new String[] {"--local true --file samples/demo.json"},
                        "samples/default.json");

        assertTrue(options.isLocal());
        assertTrue(options.hasFileOption());
        assertEquals("samples/demo.json", options.getSourcePath());
    }

    @Test
    public void fileWithoutLocalDoesNotEnableLocalMode() {
        JobSourceFactory.SourceOptions options =
                JobSourceFactory.resolveOptions(new String[] {"--file", "samples/demo.json"}, "samples/default.json");

        assertFalse(options.isLocal());
        assertTrue(options.hasFileOption());
        assertEquals("samples/demo.json", options.getSourcePath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidLocalValueFailsFast() {
        JobSourceFactory.resolveOptions(new String[] {"--local", "ture"}, "samples/default.json");
    }

    @Test
    public void localSourceKeepsConfiguredCheckpointing() {
        String previousLocalMode = System.getProperty(JobSourceFactory.LOCAL_MODE_PROPERTY);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1234L, CheckpointingMode.EXACTLY_ONCE);

        try {
            JobSourceFactory.createSourceStream(
                    env,
                    new String[] {"--local", "true"},
                    "topic",
                    "group",
                    "test",
                    "samples/商品详情页.json");

            assertTrue(env.getCheckpointConfig().isCheckpointingEnabled());
            assertEquals(1234L, env.getCheckpointConfig().getCheckpointInterval());
            assertEquals(CheckpointingMode.EXACTLY_ONCE, env.getCheckpointConfig().getCheckpointingMode());
        } finally {
            if (previousLocalMode == null) {
                System.clearProperty(JobSourceFactory.LOCAL_MODE_PROPERTY);
            } else {
                System.setProperty(JobSourceFactory.LOCAL_MODE_PROPERTY, previousLocalMode);
            }
        }
    }

    @Test
    public void localCheckpointWaitTimeoutCoversCheckpointCompletionWindow() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(3000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);

        assertEquals(4500L, JobSourceFactory.resolveLocalCheckpointWaitTimeoutMs(env));
    }

    @Test
    public void jsonFileSourceCanFinishAfterCheckpointCompletes() throws Exception {
        Path tempFile = Files.createTempFile("json-source-checkpoint", ".json");
        Files.write(tempFile, "{\"ok\":true}".getBytes(StandardCharsets.UTF_8));

        JsonFileSource source = new JsonFileSource(tempFile.toString(), 5000L);
        CountDownLatch collected = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread sourceThread = new Thread(() -> {
            try {
                source.run(new SourceFunction.SourceContext<String>() {
                    @Override
                    public void collect(String element) {
                        collected.countDown();
                    }

                    @Override
                    public void collectWithTimestamp(String element, long timestamp) {
                        collect(element);
                    }

                    @Override
                    public void emitWatermark(Watermark mark) {
                    }

                    @Override
                    public void markAsTemporarilyIdle() {
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return this;
                    }

                    @Override
                    public void close() {
                    }
                });
            } catch (Throwable throwable) {
                failure.set(throwable);
            }
        });

        try {
            sourceThread.start();
            assertTrue(collected.await(1, TimeUnit.SECONDS));
            assertTrue(sourceThread.isAlive());

            source.notifyCheckpointComplete(1L);

            sourceThread.join(1000L);
            assertFalse(sourceThread.isAlive());
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
        } finally {
            source.cancel();
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    public void jsonFileSourceOpensFileSystemPathFirst() throws Exception {
        Path tempFile = Files.createTempFile("json-source", ".json");
        Files.write(tempFile, "{\"ok\":true}".getBytes(StandardCharsets.UTF_8));

        try (InputStream inputStream = new JsonFileSource(tempFile.toString()).openInputStream()) {
            byte[] bytes = new byte[64];
            int length = inputStream.read(bytes);
            assertEquals("{\"ok\":true}", new String(bytes, 0, length, StandardCharsets.UTF_8));
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
}
