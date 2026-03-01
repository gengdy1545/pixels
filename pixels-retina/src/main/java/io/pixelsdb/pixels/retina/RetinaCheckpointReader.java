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
package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Helper class to read retina checkpoints in parallel.
 * This class encapsulates the logic for reading the footer,
 * parsing metadata, and downloading/processing parts in parallel.
 */
public class RetinaCheckpointReader
{
    private static final Logger logger = LogManager.getLogger(RetinaCheckpointReader.class);
    private static final int CHECKPOINT_MAGIC = 0x5058434B;

    static class CheckpointPartMetadata
    {
        long offset;
        int length;
        int count;

        CheckpointPartMetadata(long offset, int length, int count)
        {
            this.offset = offset;
            this.length = length;
            this.count = count;
        }
    }

    /**
     * Reads a checkpoint file using parallel range reads if possible.
     *
     * @param storage       The storage backend.
     * @param path          The path to the checkpoint file.
     * @param executor      The executor service for parallel tasks.
     * @param resultHandler A handler to process each decoded part (e.g. add to a map).
     * @return true if successful, false if fallback to legacy read is needed.
     */
    public static boolean read(Storage storage, String path, ExecutorService executor, PartResultHandler resultHandler)
    {
        try (PhysicalReader fsReader = PhysicalReaderUtil.newPhysicalReader(storage, path))
        {
            long fileLen = fsReader.getFileLength();
            if (fileLen <= 12)
            {
                return false; // Too small, likely legacy or corrupted
            }

            // 1. Read Footer (Last 12 bytes: [Magic 4B] + [FooterLen 4B])
            // Actually footer structure is: ... [Part Count 4B] [Magic 4B] [Footer Len 4B]
            // We read last 8 bytes first to check Magic and get Footer Length.
            fsReader.seek(fileLen - 8);
            int magic = fsReader.readInt(ByteOrder.BIG_ENDIAN);
            int footerLength = fsReader.readInt(ByteOrder.BIG_ENDIAN);

            if (magic != CHECKPOINT_MAGIC)
            {
                return false; // Not new format
            }

            // 2. Read and Parse Footer Metadata
            long footerStart = fileLen - footerLength;
            fsReader.seek(footerStart);
            ByteBuffer footerBuffer = fsReader.readFully(footerLength - 8); // Read up to Magic
            // footerBuffer contains: [Part Metas...] [Part Count]

            // Calculate number of parts
            // Footer Body Size = footerLength - 8 (Magic + Len)
            // Part Meta Size = 16 bytes (Long + Int + Int)
            // Footer Body = (PartMetas) + (PartCount 4B)
            // (N * 16) + 4 = FooterBodySize
            int partCount = (footerLength - 12) / 16;
            
            List<CheckpointPartMetadata> parts = new ArrayList<>(partCount);
            for (int i = 0; i < partCount; i++)
            {
                long offset = footerBuffer.getLong();
                int length = footerBuffer.getInt();
                int count = footerBuffer.getInt();
                parts.add(new CheckpointPartMetadata(offset, length, count));
            }
            int checkPartCount = footerBuffer.getInt();
            if (checkPartCount != partCount) {
                logger.warn("Part count mismatch in checkpoint footer. Calculated: {}, Recorded: {}", partCount, checkPartCount);
            }

            logger.info("Parallel loading checkpoint {} with {} parts.", path, partCount);

            // 3. Parallel Download and Parse
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (CheckpointPartMetadata part : parts)
            {
                futures.add(CompletableFuture.runAsync(() -> {
                    try
                    {
                        // We need a separate reader for each thread if the storage client isn't thread-safe for seek+read.
                        // For S3/HDFS, creating a new reader (which is lightweight connection wrapper) is usually safer/required 
                        // for concurrent range reads unless the underlying client supports concurrent pread.
                        // PhysicalReader interface generally implies stateful read (seek then read).
                        // So we create a new reader per task.
                        try (PhysicalReader partReader = PhysicalReaderUtil.newPhysicalReader(storage, path))
                        {
                            partReader.seek(part.offset);
                            ByteBuffer partBuffer = partReader.readFully(part.length);
                            
                            // Parse the part data
                            // Format: [FileId 8B] [RgId 4B] [RecordNum 4B] [BitmapLen 4B] [Bitmap...]
                            partBuffer.order(ByteOrder.BIG_ENDIAN); // Ensure endianness
                            for (int i = 0; i < part.count; i++)
                            {
                                long fileId = partBuffer.getLong();
                                int rgId = partBuffer.getInt();
                                int recordNum = partBuffer.getInt();
                                int len = partBuffer.getInt();
                                long[] bitmap = new long[len];
                                for (int j = 0; j < len; j++)
                                {
                                    bitmap[j] = partBuffer.getLong();
                                }
                                resultHandler.handle(fileId, rgId, recordNum, bitmap);
                            }
                        }
                    } catch (IOException e)
                    {
                        throw new RuntimeException("Failed to read checkpoint part", e);
                    }
                }, executor));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            return true;

        } catch (Exception e)
        {
            logger.error("Failed to read checkpoint parallelly, falling back.", e);
            return false;
        }
    }

    @FunctionalInterface
    public interface PartResultHandler
    {
        void handle(long fileId, int rgId, int recordNum, long[] bitmap);
    }
}