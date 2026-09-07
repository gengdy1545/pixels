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

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.retina.RetinaProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PixelsRecordReaderBufferImpl implements PixelsRecordReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsRecordReaderBufferImpl.class);
    private static final Long POLL_INTERVAL_MILLS = 200L;
    private static final int DEFAULT_QUEUE_CAPACITY = 16;
    private byte[] activeMemtableData;
    private final String retinaHost;
    private final List<Long> fileIds;
    private final PixelsReaderOption option;
    private final Storage storage;
    private final long tableId;
    private final String retinaBufferStorageFolder;
    private final boolean retinaEnabled;
    private final TypeDescription typeDescription;
    private final int colNum;
    private final int vectorLayout;
    private static ExecutorService prefetchExecutor; // Thread pool for I/O and deserialization
    private final BlockingQueue<PrefetchedBatch> prefetchQueue;
    private final List<Future<?>> prefetchTasks = new ArrayList<>();
    private final AtomicBoolean prefetchStarted = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger remainingBatches;
    private final AtomicInteger readRequests = new AtomicInteger(0);
    private final Object lifecycleLock = new Object();
    private final Object readLock = new Object();
    private static int maxPrefetchTasks;
    private static final int prefetchQueueCapacity = DEFAULT_QUEUE_CAPACITY; // Queue capacity
    private final boolean shouldReadHiddenColumn;
    private final List<RetinaProto.VisibilityBitmap> visibilityBitmap;
    private final List<PixelsProto.Type> includedColumnTypes;
    private final AtomicLong memoryUsage = new AtomicLong(0L);
    private final AtomicLong dataReadBytes = new AtomicLong(0L);
    /**
     * Columns included by reader option; if included, set true
     */
    private boolean[] includedColumns;
    /**
     * The ith element in resultColumns is the column id (column's index in the file schema)
     * of ith included column in the read option. The order of columns in the read option's
     * includedCols may be arbitrary, not related to the column order in schema.
     */
    private int[] resultColumns;
    /**
     * The target columns to read after matching reader option.
     * Each element represents a column id (column's index in the file schema).
     * Different from resultColumns, the ith column id in targetColumns
     * corresponds to the ith true value in this.includedColumns, i.e.,
     * The elements in targetColumns and resultColumns are in different order,
     * but they are all the index of the columns in the file schema.
     */
    private int[] targetColumns;
    private int includedColumnNum = 0;
    private long readTimeNanos = 0L;
    private boolean checkValid = false;
    private boolean activeMemtableDataEverRead = false;
    private volatile boolean endOfFile = false;
    private TypeDescription resultSchema = null;
    private long dataReadRow = 0L;
    private int vNodeId;

    private static final class PrefetchedBatch
    {
        private final int bitmapIndex;
        private final VectorizedRowBatch batch;
        private final Throwable failure;

        private PrefetchedBatch(int bitmapIndex, VectorizedRowBatch batch, Throwable failure)
        {
            this.bitmapIndex = bitmapIndex;
            this.batch = batch;
            this.failure = failure;
        }
    }

    @FunctionalInterface
    private interface PrefetchOperation
    {
        VectorizedRowBatch load() throws Exception;
    }

    public PixelsRecordReaderBufferImpl(PixelsReaderOption option,
                                        String retinaHost,
                                        byte[] activeMemtableData, List<Long> fileIds,  // read version
                                        List<RetinaProto.VisibilityBitmap> visibilityBitmap,
                                        Storage storage,
                                        long tableId, // to locate file with file id
                                        int vNodeId,
                                        TypeDescription typeDescription
    ) throws IOException
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        this.retinaBufferStorageFolder = normalizeRetinaBufferStorageFolder(
                configFactory.getProperty("retina.buffer.object.storage.folder"));
        this.retinaHost = retinaHost;
        this.retinaEnabled = Boolean.parseBoolean(configFactory.getProperty("retina.enable"));

        this.option = option;
        this.vectorLayout = (option.isReadIntColumnAsLongVector() ? TypeDescription.VectorLayout.INT_AS_LONG : 0) |
                (option.isReadShortColumnAsLongVector() ? TypeDescription.VectorLayout.SHORT_AS_LONG : 0) |
                (option.isReadTimeColumnAsLongTimeVector() ? TypeDescription.VectorLayout.TIME_AS_LONG_TIME : 0);
        this.activeMemtableData = activeMemtableData;
        this.fileIds = fileIds;
        this.storage = storage;
        this.tableId = tableId;
        this.typeDescription = typeDescription;
        this.colNum = typeDescription.getChildrenWithHiddenColumn().size();
        this.shouldReadHiddenColumn = option.hasValidTransTimestamp();
        this.visibilityBitmap = visibilityBitmap;
        this.includedColumnTypes = new ArrayList<>();
        this.vNodeId = vNodeId;
        this.prefetchQueue = new LinkedBlockingQueue<>(prefetchQueueCapacity);
        this.remainingBatches = new AtomicInteger(fileIds.size() +
                (activeMemtableData != null && activeMemtableData.length != 0 ? 1 : 0));
        initInternalExecutor();
        checkBeforeRead();
    }

    private static synchronized void initInternalExecutor()
    {
        if (prefetchExecutor == null)
        {
            ConfigFactory configFactory = ConfigFactory.Instance();
            String threadProp = configFactory.getProperty("retina.reader.prefetch.threads");
            maxPrefetchTasks = (threadProp != null) ? Integer.parseInt(threadProp) : DEFAULT_QUEUE_CAPACITY;
            prefetchExecutor = Executors.newFixedThreadPool(maxPrefetchTasks, r ->
            {
                Thread t = new Thread(r);
                t.setName("Pixels-Buffer-Reader-Prefetch-Shared");
                t.setDaemon(true);
                return t;
            });
            LOGGER.info("Initialized shared Pixels-Buffer-Reader-Prefetch pool with {} threads", maxPrefetchTasks);
        }
    }

    private static boolean checkBit(RetinaProto.VisibilityBitmap bitmap, int k)
    {
        long bitmap_ = bitmap.getBitmap(k / 64);
        return (bitmap_ & (1L << (k % 64))) != 0;
    }

    private void startPrefetching()
    {
        if (!prefetchStarted.compareAndSet(false, true) || closed.get())
        {
            return;
        }

        byte[] memtableData;
        synchronized (lifecycleLock)
        {
            if (closed.get())
            {
                return;
            }
            memtableData = activeMemtableData;
            activeMemtableData = null;
        }

        if (memtableData != null && memtableData.length != 0)
        {
            submitPrefetch(0, () -> VectorizedRowBatch.deserialize(
                    ByteBuffer.wrap(memtableData), vectorLayout));
        }

        for (int i = 0; i < fileIds.size(); ++i)
        {
            final int bitmapIndex = i + 1;
            final long fileId = fileIds.get(i);
            submitPrefetch(bitmapIndex, () ->
            {
                readRequests.incrementAndGet();
                ByteBuffer buffer = getMemtableDataFromStorage(fileId);
                return VectorizedRowBatch.deserialize(buffer, vectorLayout);
            });
        }
    }

    private void submitPrefetch(int bitmapIndex, PrefetchOperation operation)
    {
        if (closed.get())
        {
            return;
        }

        Future<?> task;
        try
        {
            task = prefetchExecutor.submit(() -> runPrefetch(bitmapIndex, operation));
        }
        catch (RuntimeException e)
        {
            enqueuePrefetchedBatch(new PrefetchedBatch(bitmapIndex, null, e));
            return;
        }

        synchronized (lifecycleLock)
        {
            if (closed.get())
            {
                task.cancel(true);
            }
            else
            {
                prefetchTasks.add(task);
            }
        }
    }

    private void runPrefetch(int bitmapIndex, PrefetchOperation operation)
    {
        VectorizedRowBatch batch = null;
        try
        {
            if (closed.get())
            {
                return;
            }

            batch = operation.load();
            if (enqueuePrefetchedBatch(new PrefetchedBatch(bitmapIndex, batch, null)))
            {
                batch = null;
            }
        }
        catch (Throwable failure)
        {
            if (!closed.get())
            {
                enqueuePrefetchedBatch(new PrefetchedBatch(bitmapIndex, null, failure));
            }
        }
        finally
        {
            if (batch != null)
            {
                batch.close();
            }
        }
    }

    private boolean enqueuePrefetchedBatch(PrefetchedBatch prefetchedBatch)
    {
        boolean interrupted = false;
        try
        {
            while (true)
            {
                synchronized (lifecycleLock)
                {
                    if (closed.get())
                    {
                        return false;
                    }
                    if (prefetchQueue.offer(prefetchedBatch))
                    {
                        if (prefetchedBatch.batch != null)
                        {
                            memoryUsage.addAndGet(prefetchedBatch.batch.getMemoryUsage());
                        }
                        return true;
                    }
                }

                try
                {
                    Thread.sleep(POLL_INTERVAL_MILLS);
                }
                catch (InterruptedException e)
                {
                    interrupted = true;
                    if (closed.get())
                    {
                        return false;
                    }
                }
            }
        }
        finally
        {
            if (interrupted)
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void checkBeforeRead() throws IOException
    {
        // filter included columns
        includedColumnNum = 0;
        String[] optionIncludedCols = option.getIncludedCols();
        // if size of cols is 0, create an empty row batch

        List<Integer> optionColsIndices = new ArrayList<>();
        this.includedColumns = new boolean[colNum];
        for (String col : optionIncludedCols)
        {
            for (int j = 0; j < colNum; j++)
            {
                if (col.equalsIgnoreCase(typeDescription.getFieldNames().get(j)))
                {
                    optionColsIndices.add(j);
                    includedColumns[j] = true;
                    includedColumnNum++;
                    break;
                }
            }
        }

        // check included columns
        if (includedColumnNum != optionIncludedCols.length && !option.isTolerantSchemaEvolution())
        {
            checkValid = false;
            throw new IOException("includedColumnsNum is " + includedColumnNum +
                    " whereas optionIncludedCols.length is " + optionIncludedCols.length);
        }

        // check retina
        if (retinaEnabled && visibilityBitmap != null && visibilityBitmap.size() != fileIds.size() + 1)
        {
            checkValid = false;
            throw new IOException("visibilityBitmap.getSize is " + visibilityBitmap.size() +
                    "except: " + fileIds.size() + 1);
        }

        // create result columns storing result column ids in user specified order
        this.resultColumns = new int[optionIncludedCols.length];
        for (int i = 0; i < optionIncludedCols.length; i++)
        {
            this.resultColumns[i] = optionColsIndices.get(i);
        }
        // assign target columns, ordered by original column order in schema
        int targetColumnNum = new HashSet<>(optionColsIndices).size();
        targetColumns = new int[targetColumnNum];
        int targetColIdx = 0;
        for (int i = 0; i < includedColumns.length; i++)
        {
            if (includedColumns[i])
            {
                targetColumns[targetColIdx] = i;
                targetColIdx++;
            }
        }
        checkValid = true;
    }

    /**
     * read() is now non-blocking and only triggers the submission of prefetch tasks.
     * It does not perform I/O or deserialization.
     */
    private boolean read() throws IOException
    {
        startPrefetching();
        if (closed.get())
        {
            endOfFile = true;
            return false;
        }
        if (remainingBatches.get() == 0)
        {
            endOfFile = true;
        }
        return checkValid;
    }

    @Override
    public int prepareBatch(int batchSize) throws IOException
    {
        return batchSize;
    }

    /**
     * Create a row batch without any data, only sets the number of rows (size) and OEF.
     * Such a row batch is used for queries such as select count(*).
     *
     * @param size the number of rows in the row batch.
     * @return the empty row batch.
     */
    private VectorizedRowBatch createEmptyRowBatch(int size)
    {
        TypeDescription resultSchema = TypeDescription.createSchema(new ArrayList<>());
        VectorizedRowBatch resultRowBatch = resultSchema.createRowBatch(0, vectorLayout);
        resultRowBatch.projectionSize = 0;
        resultRowBatch.endOfFile = this.endOfFile;
        resultRowBatch.size = size;
        return resultRowBatch;
    }

    @Override
    public VectorizedRowBatch readBatch() throws IOException
    {
        synchronized (readLock)
        {
            long start = System.nanoTime();
            if (!read())
            {
                return createEmptyRowBatch(0);
            }

            if (remainingBatches.get() == 0)
            {
                endOfFile = true;
                return createEmptyRowBatch(0);
            }

            PrefetchedBatch prefetchedBatch;
            while (true)
            {
                synchronized (lifecycleLock)
                {
                    if (closed.get())
                    {
                        endOfFile = true;
                        return createEmptyRowBatch(0);
                    }
                    prefetchedBatch = prefetchQueue.poll();
                    if (prefetchedBatch != null && prefetchedBatch.batch != null)
                    {
                        memoryUsage.addAndGet(-prefetchedBatch.batch.getMemoryUsage());
                    }
                }

                if (prefetchedBatch != null)
                {
                    break;
                }

                try
                {
                    Thread.sleep(POLL_INTERVAL_MILLS);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for prefetched batch.", e);
                }
            }

            remainingBatches.decrementAndGet();
            if (prefetchedBatch.failure != null)
            {
                close();
                throw new IOException("Failed to prefetch buffer batch with bitmap index " +
                        prefetchedBatch.bitmapIndex + ".", prefetchedBatch.failure);
            }

            VectorizedRowBatch curRowBatch = prefetchedBatch.batch;

            LongColumnVector hiddenTimestampVector = (LongColumnVector) curRowBatch.cols[this.colNum - 1];
            /**
             * construct the selected rows bitmap, size is curBatchSize
             * the i-th bit presents the curRowInRG + i row in chunkBuffers is selected or not.
             */
            int curBatchSize = curRowBatch.size;
            Bitmap selectedRows = new Bitmap(curBatchSize, false);
            for (int i = 0; i < curBatchSize; i++)
            {
                if ((hiddenTimestampVector == null || hiddenTimestampVector.vector[i] <= this.option.getTransTimestamp())
                        && (!retinaEnabled || visibilityBitmap == null ||
                        !checkBit(visibilityBitmap.get(prefetchedBatch.bitmapIndex), i)))
                {
                    selectedRows.set(i);
                }
            }
            curRowBatch.applyFilter(selectedRows);
            dataReadRow += curRowBatch.size;
            readTimeNanos += System.nanoTime() - start;
            return curRowBatch;
        }
    }

    @Override
    public TypeDescription getResultSchema()
    {
        // TODO(AntiO2): Schema evolution is currently not supported in Retina.
        return typeDescription;
    }

    @Override
    public boolean isValid()
    {
        return false;
    }

    @Override
    public boolean isEndOfFile()
    {
        return endOfFile;
    }

    @Override
    public boolean seekToRow(long rowIndex) throws IOException
    {
        return false;
    }

    @Override
    public boolean skip(long rowNum) throws IOException
    {
        return false;
    }

    @Override
    public long getCompletedRows()
    {
        return dataReadRow;
    }

    @Override
    public long getCompletedBytes()
    {
        return dataReadBytes.get();
    }

    @Override
    public int getNumReadRequests()
    {
        return readRequests.get();
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryUsage.get();
    }

    @Override
    public void close() throws IOException
    {
        List<PrefetchedBatch> remaining = new ArrayList<>();
        synchronized (lifecycleLock)
        {
            if (!closed.compareAndSet(false, true))
            {
                return;
            }

            endOfFile = true;
            activeMemtableData = null;
            for (Future<?> task : prefetchTasks)
            {
                task.cancel(true);
            }
            prefetchTasks.clear();
            prefetchQueue.drainTo(remaining);
        }

        for (PrefetchedBatch prefetchedBatch : remaining)
        {
            if (prefetchedBatch.batch != null)
            {
                memoryUsage.addAndGet(-prefetchedBatch.batch.getMemoryUsage());
                prefetchedBatch.batch.close();
            }
        }
    }

    static String normalizeRetinaBufferStorageFolder(String folder)
    {
        if (folder == null || folder.isEmpty())
        {
            throw new IllegalArgumentException("retina.buffer.object.storage.folder must not be empty");
        }
        return folder.endsWith("/") ? folder : folder + "/";
    }

    private String getRetinaBufferStoragePathFromId(long entryId, int virtualId)
    {
        return this.retinaBufferStorageFolder + String.format("%d/%d/%s_%d", tableId, virtualId, retinaHost, entryId);
    }

    ByteBuffer getMemtableDataFromStorage(long fileId) throws IOException
    {
        String path = getRetinaBufferStoragePathFromId(fileId, vNodeId);
        return getMemtableDataFromStorage(path);
    }

    private ByteBuffer getMemtableDataFromStorage(String path) throws IOException
    {
        // Polling loop for file existence runs inside the prefetchExecutor's worker thread.
        while (!closed.get())
        {

            if (storage.exists(path))
            {
                try (PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(storage, path))
                {
                    int length = (int) reader.getFileLength();
                    dataReadBytes.addAndGet(length);
                    return reader.readFully(length);
                }
            }

            try
            {
                Thread.sleep(POLL_INTERVAL_MILLS);
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for file existence: " + path, e);
            }
        }
        throw new IOException("Buffer reader closed while waiting for file: " + path);
    }


    @Override
    public VectorizedRowBatch readBatch(int batchSize, boolean reuse) throws IOException
    {
        return readBatch();
    }

    @Override
    public VectorizedRowBatch readBatch(int batchSize) throws IOException
    {
        return readBatch();
    }

    @Override
    public VectorizedRowBatch readBatch(boolean reuse) throws IOException
    {
        return readBatch();
    }
}
