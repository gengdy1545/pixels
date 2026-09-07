/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.retina.RetinaProto;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestPixelsRecordReaderBufferPrefetch
{
    private static final TypeDescription SCHEMA = TypeDescription.fromString("struct<value:long>");

    @BeforeClass
    public static void configureReader()
    {
        ConfigFactory config = ConfigFactory.Instance();
        config.addProperty("retina.enable", "true");
        config.addProperty("retina.buffer.object.storage.folder", "/tmp/pixels-buffer-reader-test");
        config.addProperty("retina.reader.prefetch.threads", "4");
    }

    @Test(timeout = 10000)
    public void testOutOfOrderBatchesUseTheirOwnBitmap() throws Exception
    {
        byte[] activeBatch = createBatch(10, 11);
        byte[] firstObjectBatch = createBatch(20, 21);
        byte[] secondObjectBatch = createBatch(30, 31);
        CountDownLatch firstObjectStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstObject = new CountDownLatch(1);

        StubBufferReader reader = new StubBufferReader(
                activeBatch,
                Arrays.asList(1L, 2L),
                Arrays.asList(bitmap(0b01L), bitmap(0b10L), bitmap(0b01L)),
                fileId ->
                {
                    if (fileId == 1L)
                    {
                        firstObjectStarted.countDown();
                        await(releaseFirstObject);
                        return ByteBuffer.wrap(firstObjectBatch);
                    }
                    assertTrue(firstObjectStarted.await(5, TimeUnit.SECONDS));
                    return ByteBuffer.wrap(secondObjectBatch);
                });

        List<Long> values = new ArrayList<>();
        try
        {
            while (!values.contains(31L))
            {
                VectorizedRowBatch batch = reader.readBatch();
                assertTrue(!batch.endOfFile);
                values.add(firstValue(batch));
                batch.close();
            }
            releaseFirstObject.countDown();

            while (values.size() < 3)
            {
                VectorizedRowBatch batch = reader.readBatch();
                assertTrue(!batch.endOfFile);
                values.add(firstValue(batch));
                batch.close();
            }

            Set<Long> expected = new HashSet<>(Arrays.asList(11L, 20L, 31L));
            assertEquals(expected, new HashSet<>(values));

            VectorizedRowBatch eof = reader.readBatch();
            assertTrue(eof.endOfFile);
            eof.close();
            assertTrue(reader.isEndOfFile());
        }
        finally
        {
            releaseFirstObject.countDown();
            reader.close();
        }
    }

    @Test(timeout = 10000)
    public void testPrefetchFailureIsPropagated() throws Exception
    {
        IOException expected = new IOException("injected prefetch failure");
        StubBufferReader reader = new StubBufferReader(
                new byte[0],
                Arrays.asList(1L),
                Arrays.asList(bitmap(0L), bitmap(0L)),
                fileId ->
                {
                    throw expected;
                });

        try
        {
            reader.readBatch();
            fail("expected prefetch failure");
        }
        catch (IOException actual)
        {
            assertTrue(actual.getMessage().contains("bitmap index 1"));
            assertEquals(expected, actual.getCause());
            assertTrue(reader.isEndOfFile());
            assertEquals(0L, reader.getMemoryUsage());
        }
        finally
        {
            reader.close();
        }
    }

    @Test(timeout = 10000)
    public void testCloseCancelsTasksAndReleasesQueuedBatches() throws Exception
    {
        byte[] batchData = createBatch(40, 41);
        CountDownLatch blockedTaskStarted = new CountDownLatch(1);
        CountDownLatch blockedTaskInterrupted = new CountDownLatch(1);
        CountDownLatch releaseBlockedTask = new CountDownLatch(1);
        StubBufferReader reader = new StubBufferReader(
                new byte[0],
                Arrays.asList(1L, 2L, 3L),
                Arrays.asList(bitmap(0L), bitmap(0L), bitmap(0L), bitmap(0L)),
                fileId ->
                {
                    if (fileId == 2L)
                    {
                        blockedTaskStarted.countDown();
                        try
                        {
                            releaseBlockedTask.await();
                        }
                        catch (InterruptedException e)
                        {
                            blockedTaskInterrupted.countDown();
                            Thread.currentThread().interrupt();
                            throw new IOException("interrupted", e);
                        }
                    }
                    return ByteBuffer.wrap(batchData);
                });

        try
        {
            VectorizedRowBatch returned = reader.readBatch();
            returned.close();
            assertTrue(blockedTaskStarted.await(5, TimeUnit.SECONDS));
            waitForQueuedBatch(reader);

            reader.close();

            assertTrue(blockedTaskInterrupted.await(5, TimeUnit.SECONDS));
            assertEquals(0L, reader.getMemoryUsage());
            VectorizedRowBatch eof = reader.readBatch();
            assertTrue(eof.endOfFile);
            eof.close();
        }
        finally
        {
            releaseBlockedTask.countDown();
            reader.close();
        }
    }

    private static PixelsReaderOption readerOption()
    {
        return new PixelsReaderOption()
                .includeCols(new String[]{"value"})
                .transTimestamp(100L);
    }

    private static byte[] createBatch(long first, long second)
    {
        VectorizedRowBatch batch = SCHEMA.createRowBatchWithHiddenColumn(2);
        LongColumnVector values = (LongColumnVector) batch.cols[0];
        LongColumnVector timestamps = (LongColumnVector) batch.cols[1];
        values.add(first);
        values.add(second);
        timestamps.add(0L);
        timestamps.add(0L);
        batch.size = 2;
        byte[] serialized = batch.serialize();
        batch.close();
        return serialized;
    }

    private static RetinaProto.VisibilityBitmap bitmap(long bits)
    {
        return RetinaProto.VisibilityBitmap.newBuilder().addBitmap(bits).build();
    }

    private static long firstValue(VectorizedRowBatch batch)
    {
        assertEquals(1, batch.size);
        return ((LongColumnVector) batch.cols[0]).vector[0];
    }

    private static void waitForQueuedBatch(PixelsRecordReaderBufferImpl reader) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (reader.getMemoryUsage() == 0 && System.nanoTime() < deadline)
        {
            Thread.sleep(10);
        }
        assertTrue("a prefetched batch should be queued", reader.getMemoryUsage() > 0);
    }

    private static void await(CountDownLatch latch) throws IOException
    {
        try
        {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        }
    }

    @FunctionalInterface
    private interface BufferLoader
    {
        ByteBuffer load(long fileId) throws Exception;
    }

    private static final class StubBufferReader extends PixelsRecordReaderBufferImpl
    {
        private final BufferLoader loader;

        private StubBufferReader(
                byte[] activeMemtableData,
                List<Long> fileIds,
                List<RetinaProto.VisibilityBitmap> bitmaps,
                BufferLoader loader) throws IOException
        {
            super(readerOption(), "retina-test", activeMemtableData, fileIds, bitmaps,
                    null, 1L, 0, SCHEMA);
            this.loader = loader;
        }

        @Override
        ByteBuffer getMemtableDataFromStorage(long fileId) throws IOException
        {
            try
            {
                return loader.load(fileId);
            }
            catch (IOException e)
            {
                throw e;
            }
            catch (Exception e)
            {
                throw new IOException(e);
            }
        }
    }
}
