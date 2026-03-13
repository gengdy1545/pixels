/*
 * Copyright 2026 PixelsDB.
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

/**
 * Storage Garbage Collector for Pixels Retina.
 *
 * This class implements the Storage GC mechanism that reclaims physical storage
 * by rewriting files with high invalid ratios. It coordinates with Memory GC
 * to ensure safe deletion of data while maintaining MVCC semantics.
 *
 * Key features:
 * - Scans files based on invalid ratio threshold
 * - Groups files by (tableId, retinaNodeId) for merging
 * - Rewrites data using Base Bitmap (not full Visibility)
 * - Migrates Deletion Chain to new files
 * - Uses dual-write strategy during index updates
 * - Atomically swaps metadata
 *
 * Test command:
 * mvn test -Dtest=StorageGarbageCollectorTest
 *
 * Configuration:
 * - storage.gc.enabled: Enable/disable Storage GC
 * - storage.gc.threshold: Invalid ratio threshold (default 0.5)
 * - storage.gc.target.file.size: Target file size after rewrite (default 128MB)
 * - storage.gc.max.files.per.run: Max files to process per GC run (default 10)
 */
package io.pixelsdb.pixels.retina;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.exception.MainIndexException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RowIdException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.index.service.LocalIndexService;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.metadata.domain.Schema;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.IndexUtils;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.Collections;
import java.util.stream.Collectors;

public class StorageGarbageCollector
{
    private static final Logger logger = LogManager.getLogger(StorageGarbageCollector.class);
    // Number of retry attempts for MainIndex sync before aborting the rewrite.
    private static final int SYNC_MAX_RETRIES = 3;

    private final RetinaResourceManager resourceManager;
    private final MetadataService metadataService;
    private final Storage storage;
    private final GcWal gcWal;

    // Configuration parameters
    private final boolean enabled;
    private final double invalidRatioThreshold;
    private final long targetFileSize;
    private final int maxFilesPerRun;

    public StorageGarbageCollector(RetinaResourceManager resourceManager,
                                   MetadataService metadataService,
                                   GcWal gcWal)
    {
        this.resourceManager = resourceManager;
        this.metadataService = metadataService;
        this.gcWal = gcWal;

        ConfigFactory config = ConfigFactory.Instance();
        String enabledStr = config.getProperty("storage.gc.enabled");
        this.enabled = enabledStr != null ? Boolean.parseBoolean(enabledStr) : true;
        String thresholdStr = config.getProperty("storage.gc.threshold");
        this.invalidRatioThreshold = thresholdStr != null ? Double.parseDouble(thresholdStr) : 0.5;
        String targetFileSizeStr = config.getProperty("storage.gc.target.file.size");
        this.targetFileSize = targetFileSizeStr != null ? Long.parseLong(targetFileSizeStr) : 134217728L; // 128MB
        String maxFilesStr = config.getProperty("storage.gc.max.files.per.run");
        this.maxFilesPerRun = maxFilesStr != null ? Integer.parseInt(maxFilesStr) : 10;

        try
        {
            this.storage = StorageFactory.Instance().getStorage(
                    config.getProperty("pixels.storage.scheme"));
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to initialize storage for Storage GC", e);
        }

        logger.info("StorageGarbageCollector initialized: enabled={}, threshold={}, targetFileSize={}, maxFiles={}",
                enabled, invalidRatioThreshold, targetFileSize, maxFilesPerRun);
    }

    /**
     * Main entry point for Storage GC.
     * Should be called after Memory GC completes.
     */
    public void runStorageGC()
    {
        if (!enabled)
        {
            logger.debug("Storage GC is disabled");
            return;
        }

        logger.info("Starting Storage GC...");
        long startTime = System.currentTimeMillis();

        try
        {
            // Phase 1: Scan and group files
            List<FileGroup> fileGroups = scanAndGroupFiles();

            if (fileGroups.isEmpty())
            {
                logger.info("No files need Storage GC");
                return;
            }

            logger.info("Found {} file groups for Storage GC", fileGroups.size());

            // Phase 2: Rewrite each group
            int successCount = 0;
            int failCount = 0;
            for (FileGroup group : fileGroups)
            {
                try
                {
                    rewriteFileGroup(group);
                    successCount++;
                }
                catch (Exception e)
                {
                    logger.error("Failed to rewrite file group: " + group, e);
                    failCount++;
                    // Continue with next group
                }
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Storage GC completed: success={}, failed={}, duration={}ms",
                    successCount, failCount, duration);
        }
        catch (Exception e)
        {
            logger.error("Storage GC failed", e);
        }
    }

    /**
     * Scan files and group by (tableId, retinaNodeId).
     *
     * Algorithm:
     * 1. Get all files from metadata
     * 2. For each file, calculate invalid ratio from RGVisibility
     * 3. Filter files above threshold
     * 4. Group by (tableId, retinaNodeId)
     * 5. Merge groups until target file size
     */
    private List<FileGroup> scanAndGroupFiles()
    {
        logger.info("Scanning files for Storage GC...");

        try
        {
            // Step 1: Get all schemas and tables
            List<Schema> schemas = metadataService.getSchemas();
            Map<String, List<FileCandidate>> groupMap = new HashMap<>();

            for (Schema schema : schemas)
            {
                List<Table> tables = metadataService.getTables(schema.getName());

                for (Table table : tables)
                {
                    // Step 2: Get all layouts for this table, then paths from each layout
                    List<Layout> layouts = metadataService.getLayouts(schema.getName(), table.getName());

                    for (Layout layout : layouts)
                    {
                        List<Path> paths = metadataService.getPaths(layout.getId(), true);

                        for (Path path : paths)
                        {
                            // Step 3: Get all files for this path
                            List<File> files = metadataService.getFiles(path.getId());

                            for (File file : files)
                            {
                                // Compute full file path and size
                                String filePath = File.getFilePath(path, file);
                                long fileSize;
                                try
                                {
                                    fileSize = storage.getStatus(filePath).getLength();
                                }
                                catch (IOException e)
                                {
                                    logger.warn("Failed to get file size for {}", filePath, e);
                                    fileSize = 0;
                                }

                                // Step 4: Calculate invalid ratio
                                double fileInvalidRatio = calculateFileInvalidRatio(file);

                                if (fileInvalidRatio > invalidRatioThreshold)
                                {
                                    // Step 5: Extract retinaNodeId from file name
                                    String retinaNodeId = RetinaUtils.extractRetinaNodeIdFromPath(file.getName());

                                    // Step 6: Group by (tableId, retinaNodeId)
                                    String groupKey = table.getId() + "_" + retinaNodeId;
                                    groupMap.computeIfAbsent(groupKey, k -> new ArrayList<>())
                                            .add(new FileCandidate(file, path, filePath, fileSize,
                                                    table.getId(), retinaNodeId, fileInvalidRatio));

                                    logger.debug("File {} marked for GC: invalidRatio={}",
                                            file.getName(), fileInvalidRatio);
                                }
                            }
                        }
                    }
                }
            }

            // Step 7: Merge groups to reach target file size
            List<FileGroup> fileGroups = new ArrayList<>();
            for (Map.Entry<String, List<FileCandidate>> entry : groupMap.entrySet())
            {
                List<FileCandidate> candidates = entry.getValue();
                // Sort by invalid ratio (descending) to prioritize high-ratio files
                candidates.sort((a, b) -> Double.compare(b.invalidRatio, a.invalidRatio));

                // Limit to maxFilesPerRun
                int filesToProcess = Math.min(candidates.size(), maxFilesPerRun);
                List<FileCandidate> groupFiles = new ArrayList<>();
                long totalSize = 0;

                for (int i = 0; i < filesToProcess; i++)
                {
                    FileCandidate candidate = candidates.get(i);
                    groupFiles.add(candidate);
                    totalSize += candidate.fileSize;

                    // Create a group if we reach target size or max files
                    if (totalSize >= targetFileSize || groupFiles.size() >= maxFilesPerRun)
                    {
                        fileGroups.add(new FileGroup(candidate.tableId, candidate.retinaNodeId, new ArrayList<>(groupFiles)));
                        groupFiles.clear();
                        totalSize = 0;
                    }
                }

                // Add remaining files as a group
                if (!groupFiles.isEmpty())
                {
                    FileCandidate first = candidates.get(0);
                    fileGroups.add(new FileGroup(first.tableId, first.retinaNodeId, groupFiles));
                }
            }

            logger.info("Scanned {} file groups for Storage GC", fileGroups.size());
            return fileGroups;
        }
        catch (Exception e)
        {
            logger.error("Failed to scan files for Storage GC", e);
            return new ArrayList<>();
        }
    }

    /**
     * Rewrite a group of files.
     *
     * Steps:
     * 1. Create new file with PixelsWriter
     * 2. Read old files with PixelsReader
     * 3. Filter data using Base Bitmap (not full Visibility)
     * 4. Generate RowId Mapping
     * 5. Export and migrate Deletion Chain
     * 6. Register dual-write mapping
     * 7. Update indexes
     * 8. Atomic swap metadata
     * 9. Unregister mapping
     */
    private void rewriteFileGroup(FileGroup group) throws Exception
    {
        logger.info("Rewriting file group: {}", group);

        // ── Step 1: Generate path; pre-register new file as TEMPORARY ────────────
        // Pre-registering early gives us a stable, real newFileId for RGVisibility,
        // redirect maps, and MainIndex — avoiding the post-swap fileId-update dance.
        String newFilePath = generateNewFilePath(group);
        logger.info("Creating new file: {}", newFilePath);

        File newFileMeta = new File();
        newFileMeta.setName(newFilePath.substring(newFilePath.lastIndexOf('/') + 1));
        newFileMeta.setType(File.Type.TEMPORARY);
        newFileMeta.setNumRowGroup(0);
        newFileMeta.setPathId(group.files.get(0).path.getId());
        metadataService.addFiles(Collections.singletonList(newFileMeta));
        long newFileId = metadataService.getFileId(newFilePath);
        newFileMeta.setId(newFileId);

        // WAL: persist PENDING entry before any further metadata/physical changes so that
        // a crash between here and the atomic swap can be detected and cleaned up on restart.
        try
        {
            gcWal.beginEntry(newFileId, newFilePath);
        }
        catch (IOException e)
        {
            // WAL write failure is fatal for this rewrite: clean up and abort.
            try { metadataService.deleteFiles(Collections.singletonList(newFileId)); } catch (Exception ignored) { }
            throw new IOException("GcWal beginEntry failed for newFileId=" + newFileId
                    + "; aborting rewrite to avoid un-recoverable state", e);
        }

        // ── Step 2: Read schema from first old file ──────────────────────────────
        TypeDescription schema;
        long pixelStride;
        PixelsProto.CompressionKind compressionKind;
        int rowGroupSize;
        EncodingLevel encodingLevel;
        boolean hasHiddenCol;
        try (PixelsReader firstReader = PixelsReaderImpl.newBuilder()
                .setStorage(storage)
                .setPath(group.files.get(0).filePath)
                .build())
        {
            schema = firstReader.getFileSchema();
            pixelStride = firstReader.getPixelStride();
            compressionKind = firstReader.getCompressionKind();
            rowGroupSize = getRowGroupSizeFromConfig(firstReader);
            encodingLevel = getEncodingLevelFromConfig(firstReader);
            hasHiddenCol = firstReader.getPostScript().getHasHiddenColumn();
        }

        // Load primary index metadata for SinglePointIndex sync.
        // If the table has no primary index or the file has no hidden timestamp column,
        // we cannot reconstruct IndexKey and skip the sync.
        SinglePointIndex primaryIndexMeta = null;
        int[] pkColSchemaIndices = null;
        TypeDescription.Category[] pkColCategories = null;
        if (hasHiddenCol)
        {
            try
            {
                primaryIndexMeta = metadataService.getPrimaryIndex(group.tableId);
                if (primaryIndexMeta != null)
                {
                    int[][] pkInfo = resolvePkColumns(primaryIndexMeta, schema);
                    pkColSchemaIndices = pkInfo[0];
                    // pkInfo[1] holds Category ordinals - convert back to Category[]
                    pkColCategories = new TypeDescription.Category[pkInfo[1].length];
                    List<TypeDescription> schemaFields = schema.getChildren();
                    for (int pi = 0; pi < pkColSchemaIndices.length; pi++)
                    {
                        pkColCategories[pi] = schemaFields.get(pkColSchemaIndices[pi]).getCategory();
                    }
                }
            }
            catch (MetadataException e)
            {
                logger.warn("Failed to load primary index metadata for tableId={}, SinglePointIndex sync will be skipped: {}",
                        group.tableId, e.getMessage());
            }
        }

        boolean syncSPI = (primaryIndexMeta != null) && hasHiddenCol;
        // Per-surviving-row data for SinglePointIndex sync, indexed by newGlobalRowId order.
        List<byte[]> newRowPkBytes = syncSPI ? new ArrayList<>() : null;
        List<Long>   newRowWriteTs = syncSPI ? new ArrayList<>() : null;

        String[] allColNames = schema.getFieldNames().toArray(new String[0]);

        // ── Step 3: Write surviving rows to the new file ─────────────────────────
        PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride((int) pixelStride)
                .setRowGroupSize(rowGroupSize)
                .setStorage(storage)
                .setPath(newFilePath)
                .setCompressionKind(compressionKind)
                .setEncodingLevel(encodingLevel)
                .setHasHiddenColumn(hasHiddenCol)
                .build();

        Map<Long, int[]> fileMappings = new HashMap<>();
        int globalNewRowId = 0;
        boolean writerClosed = false;
        try
        {
            for (FileCandidate oldCandidate : group.files)
            {
                File oldFile = oldCandidate.file;
                logger.info("Processing file: {}", oldFile.getName());

                try (PixelsReader reader = PixelsReaderImpl.newBuilder()
                        .setStorage(storage)
                        .setPath(oldCandidate.filePath)
                        .build())
                {
                    int numRowGroups = reader.getRowGroupNum();
                    int[] fileMapping = new int[(int) reader.getNumberOfRows()];
                    Arrays.fill(fileMapping, -1);

                    for (int rgId = 0; rgId < numRowGroups; rgId++)
                    {
                        RGVisibility rgVisibility = resourceManager.getRGVisibility(oldFile.getId(), rgId);
                        if (rgVisibility == null)
                        {
                            logger.warn("RGVisibility not found for fileId={}, rgId={}, skipping",
                                    oldFile.getId(), rgId);
                            continue;
                        }

                        long[] baseBitmap = rgVisibility.getBaseBitmap();
                        PixelsReaderOption option = new PixelsReaderOption();
                        option.skipCorruptRecords(true);
                        option.tolerantSchemaEvolution(true);
                        option.rgRange(rgId, 1);
                        option.includeCols(allColNames);
                        if (hasHiddenCol)
                        {
                            option.forceReadHiddenColumn(true);
                        }

                        int rgRowOffset = 0;
                        try (PixelsRecordReader recordReader = reader.read(option))
                        {
                            VectorizedRowBatch rowBatch;
                            while ((rowBatch = recordReader.readBatch()) != null)
                            {
                                int batchSize = rowBatch.size;
                                if (batchSize == 0)
                                {
                                    break;
                                }

                                long[] batchTimestamps = hasHiddenCol
                                        ? recordReader.getLastBatchTimestamps() : null;

                                // Create write batch; when hasHiddenCol the last ColumnVector
                                // slot is reserved for the hidden timestamp.
                                VectorizedRowBatch filteredBatch = createWriteBatch(schema, batchSize, hasHiddenCol);
                                int filteredCount = 0;

                                for (int i = 0; i < batchSize; i++)
                                {
                                    // Base Bitmap: 1 = deleted/invalid, 0 = valid
                                    int globalRowId = rgRowOffset + i;
                                    int bitmapIndex = globalRowId / 64;
                                    int bitOffset = globalRowId % 64;
                                    boolean isDeleted = (baseBitmap[bitmapIndex] & (1L << bitOffset)) != 0;

                                    if (!isDeleted)
                                    {
                                        copyRow(rowBatch, i, filteredBatch, filteredCount);
                                        // Propagate hidden timestamp to new file.
                                        if (hasHiddenCol && batchTimestamps != null)
                                        {
                                            ((LongColumnVector) filteredBatch.cols[filteredBatch.cols.length - 1])
                                                    .vector[filteredCount] = batchTimestamps[i];
                                        }
                                        fileMapping[globalRowId] = globalNewRowId++;
                                        // Collect pk bytes + write timestamp for SinglePointIndex sync.
                                        if (syncSPI && batchTimestamps != null)
                                        {
                                            newRowWriteTs.add(batchTimestamps[i]);
                                            newRowPkBytes.add(serializePkBytes(
                                                    rowBatch, i, pkColSchemaIndices, pkColCategories));
                                        }
                                        filteredCount++;
                                    }
                                }

                                if (filteredCount > 0)
                                {
                                    filteredBatch.size = filteredCount;
                                    pixelsWriter.addRowBatch(filteredBatch);
                                }
                                rgRowOffset += batchSize;
                            }
                        }
                    }
                    fileMappings.put(oldFile.getId(), fileMapping);
                }
            }

            pixelsWriter.close();
            writerClosed = true;
        }
        catch (Exception e)
        {
            if (!writerClosed)
            {
                try { pixelsWriter.close(); } catch (Exception ignored) { }
            }
            try { storage.delete(newFilePath, false); } catch (Exception ignored) { }
            try { metadataService.deleteFiles(Collections.singletonList(newFileId)); } catch (Exception ignored) { }
            throw e;
        }

        int totalValidRows = globalNewRowId;
        int numNewRGsWritten = pixelsWriter.getNumRowGroup();

        // ── Step 4: Migrate deletion chains; create new RGVisibility ─────────────
        Map<Integer, long[]> newRGDeletions = new HashMap<>();
        for (FileCandidate oldCandidate : group.files)
        {
            File oldFile = oldCandidate.file;
            int[] mapping = fileMappings.get(oldFile.getId());
            if (mapping == null) continue;

            for (int rgId = 0; rgId < oldFile.getNumRowGroup(); rgId++)
            {
                RGVisibility oldRgVisibility = resourceManager.getRGVisibility(oldFile.getId(), rgId);
                if (oldRgVisibility == null) continue;

                long[] convertedBlocks = convertDeletionBlocks(
                        oldRgVisibility.exportDeletionBlocks(), mapping);

                if (convertedBlocks.length > 0)
                {
                    int newRgId = rgId;  // simplified: assumes same RG structure
                    long[] existing = newRGDeletions.getOrDefault(newRgId, new long[0]);
                    long[] merged = new long[existing.length + convertedBlocks.length];
                    System.arraycopy(existing, 0, merged, 0, existing.length);
                    System.arraycopy(convertedBlocks, 0, merged, existing.length, convertedBlocks.length);
                    newRGDeletions.put(newRgId, merged);
                    logger.debug("Migrated {} deletion blocks from oldFile={}, rgId={}",
                            convertedBlocks.length, oldFile.getId(), rgId);
                }
            }
        }

        int approxRecordsPerRG = numNewRGsWritten > 0
                ? Math.max(1, totalValidRows / numNewRGsWritten)
                : totalValidRows;
        for (Map.Entry<Integer, long[]> entry : newRGDeletions.entrySet())
        {
            RGVisibility newRgVisibility = resourceManager.createRGVisibility(
                    newFileId, entry.getKey(), approxRecordsPerRG);
            newRgVisibility.prependDeletionBlocks(entry.getValue());
        }

        // ── Step 5: Register dual-write ──────────────────────────────────────────
        // registerRedirection sets both forwardMap and backwardMap internally.
        for (Map.Entry<Long, int[]> entry : fileMappings.entrySet())
        {
            resourceManager.registerRedirection(entry.getKey(), newFileId, entry.getValue());
        }
        // Dual-write is now active (t1).

        // ── Step 6: Sync MainIndex (abort before activation if this fails) ────────
        List<Long> filesToDelete = group.files.stream()
                .map(c -> c.file.getId())
                .collect(Collectors.toList());

        long firstNewRowId = -1;
        try
        {
            firstNewRowId = syncMainIndex(group, fileMappings, newFileId, newFilePath, totalValidRows);
        }
        catch (MainIndexException e)
        {
            logger.error("MainIndex sync failed for newFileId={}; aborting rewrite of group {}",
                    newFileId, group, e);
            for (Long oldFileId : filesToDelete)
            {
                resourceManager.unregisterRedirection(oldFileId, newFileId);
            }
            resourceManager.removeAllRGVisibilityForFile(newFileId);
            try { storage.delete(newFilePath, false); } catch (Exception ignored) { }
            try { metadataService.deleteFiles(Collections.singletonList(newFileId)); } catch (Exception ignored) { }
            throw e;
        }

        // ── Step 6b: Sync SinglePointIndex (best-effort; failures are logged, not fatal) ──
        if (syncSPI && firstNewRowId >= 0 && newRowPkBytes != null && !newRowPkBytes.isEmpty())
        {
            try
            {
                syncSinglePointIndex(group, primaryIndexMeta, firstNewRowId,
                        newRowPkBytes, newRowWriteTs, newFileId);
            }
            catch (Exception e)
            {
                logger.error("SinglePointIndex sync failed for newFileId={}; index may be stale until next purge. group={}",
                        newFileId, group, e);
                // Non-fatal: the old entries will eventually be cleaned up by purgeIndexEntries.
                // The new entries are missing but queries may fall back to slower paths.
            }
        }

        // ── Step 7: Atomically activate new file and retire old ones ─────────────
        // atomicSwapFiles executes DELETE [oldFileIds] + UPDATE [TEMPORARY→REGULAR]
        // in a single DB transaction, eliminating the crash window that existed
        // with the previous separate deleteFiles + updateFile calls.
        newFileMeta.setType(File.Type.REGULAR);
        newFileMeta.setNumRowGroup(numNewRGsWritten);
        try
        {
            metadataService.atomicSwapFiles(Collections.singletonList(newFileMeta), filesToDelete);
        }
        catch (Exception e)
        {
            // atomicSwapFiles failed; old metadata is untouched, so we can safely roll back.
            logger.error("atomicSwapFiles failed for newFileId={}; rolling back rewrite of group {}",
                    newFileId, group, e);
            for (Long oldFileId : filesToDelete)
            {
                resourceManager.unregisterRedirection(oldFileId, newFileId);
            }
            resourceManager.removeAllRGVisibilityForFile(newFileId);
            try { storage.delete(newFilePath, false); } catch (Exception ignored) { }
            try { metadataService.deleteFiles(Collections.singletonList(newFileId)); } catch (Exception ignored) { }
            throw e;
        }

        // WAL: record successful completion so restart recovery skips this entry.
        try
        {
            gcWal.commitEntry(newFileId);
        }
        catch (IOException e)
        {
            // A missing DONE record only causes a spurious (but safe) cleanup attempt on restart;
            // the rewrite itself succeeded, so we only warn rather than failing the operation.
            logger.warn("GcWal commitEntry failed for newFileId={}; recovery may attempt a no-op cleanup on restart",
                    newFileId, e);
        }
        logger.info("Rewrite succeeded for file group: {} old files → {}", group.files.size(), newFilePath);

        // ── Step 8: Unregister dual-write ─────────────────────────────────────────
        for (Long oldFileId : filesToDelete)
        {
            resourceManager.unregisterRedirection(oldFileId, newFileId);
        }
    }

    // =========================================================================
    // MainIndex sync
    // =========================================================================

    /**
     * Write fresh {@link MainIndex} entries for every surviving row in the rewritten file.
     *
     * <h3>Retry semantics</h3>
     * <ul>
     *   <li>Row-ID batch allocation ({@code allocateRowIdBatch}) is done <em>once</em> before
     *       the retry loop.  If allocation fails the exception is propagated immediately.</li>
     *   <li>{@code putEntry} + {@code flushCache} may be retried up to
     *       {@link #SYNC_MAX_RETRIES} times with exponential back-off.  Because
     *       {@code putEntry} is idempotent (same rowId → same RowLocation on each attempt),
     *       retrying with the same batch is safe.</li>
     *   <li>If all retry attempts are exhausted a {@link MainIndexException} is thrown, which
     *       causes the caller to abort the rewrite <em>before</em> activating the new file,
     *       leaving old files and their metadata intact.</li>
     * </ul>
     *
     * @param group          the file group being rewritten (provides {@code tableId})
     * @param fileMappings   per-old-file mapping: {@code mapping[oldRgLocalRowId] = newGlobalRowId}
     *                       (or {@code -1} for physically deleted rows)
     * @param newFileId      real metadata file ID of the new file (pre-registered as TEMPORARY)
     * @param newFilePath    full URI path of the newly written file (used to read its footer)
     * @param totalValidRows total number of surviving rows (= sum of non-negative mapping values)
     * @throws MainIndexException if sync cannot be completed after all retry attempts
     */
    /**
     * Returns the first new row ID allocated for the rewritten file,
     * or -1 when there are no surviving rows.
     */
    private long syncMainIndex(FileGroup group,
                               Map<Long, int[]> fileMappings,
                               long newFileId,
                               String newFilePath,
                               int totalValidRows) throws MainIndexException
    {
        if (totalValidRows == 0)
        {
            return -1L;
        }

        // Read the new file's footer to obtain per-RG row counts, needed to convert
        // a newGlobalRowId into a (newRgId, newRgLocalOffset) RowLocation.
        int[] newRgCumulativeRows;
        try (PixelsReader newReader = PixelsReaderImpl.newBuilder()
                .setStorage(storage).setPath(newFilePath).build())
        {
            int numNewRGs = newReader.getRowGroupNum();
            int[] rowCounts = new int[numNewRGs];
            for (int i = 0; i < numNewRGs; i++)
            {
                rowCounts[i] = newReader.getRowGroupInfo(i).getNumberOfRows();
            }
            newRgCumulativeRows = buildCumulativeArray(rowCounts);
        }
        catch (Exception e)
        {
            throw new MainIndexException("Failed to read new file footer for MainIndex sync", e);
        }

        // Obtain the MainIndex instance for this table.
        MainIndex mainIndex;
        try
        {
            mainIndex = MainIndexFactory.Instance().getMainIndex(group.tableId);
        }
        catch (Exception e)
        {
            throw new MainIndexException("Failed to get MainIndex for tableId=" + group.tableId, e);
        }

        // Allocate a contiguous row-ID batch (done once; no retry for allocation).
        IndexProto.RowIdBatch rowIdBatch;
        try
        {
            rowIdBatch = mainIndex.allocateRowIdBatch(group.tableId, totalValidRows);
        }
        catch (RowIdException e)
        {
            throw new MainIndexException("Failed to allocate row ID batch for tableId=" + group.tableId, e);
        }
        long firstNewRowId = rowIdBatch.getRowIdStart();

        // Retry loop: putEntry for every surviving row, then flush.
        MainIndexException lastException = null;
        for (int attempt = 1; attempt <= SYNC_MAX_RETRIES; attempt++)
        {
            try
            {
                for (FileCandidate oldCandidate : group.files)
                {
                    int[] mapping = fileMappings.get(oldCandidate.file.getId());
                    if (mapping == null) continue;
                    for (int oldRowId = 0; oldRowId < mapping.length; oldRowId++)
                    {
                        int newGlobal = mapping[oldRowId];
                        if (newGlobal < 0) continue;  // row was physically deleted

                        long newRowId = firstNewRowId + newGlobal;
                        int newRgId = findNewRgId(newGlobal, newRgCumulativeRows);
                        int newRgLocalOffset = newGlobal - newRgCumulativeRows[newRgId];

                        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                                .setFileId(newFileId)
                                .setRgId(newRgId)
                                .setRgRowOffset(newRgLocalOffset)
                                .build();

                        if (!mainIndex.putEntry(newRowId, location))
                        {
                            throw new MainIndexException(
                                    "putEntry returned false for rowId=" + newRowId);
                        }
                    }
                }
                mainIndex.flushCache(newFileId);
                logger.info("MainIndex sync complete for newFileId={}: {} entries written",
                        newFileId, totalValidRows);
                return firstNewRowId;  // success
            }
            catch (MainIndexException e)
            {
                lastException = e;
                if (attempt < SYNC_MAX_RETRIES)
                {
                    long delayMs = 1000L * attempt;
                    logger.warn("MainIndex sync attempt {}/{} failed for newFileId={}; retrying in {}ms",
                            attempt, SYNC_MAX_RETRIES, newFileId, delayMs, e);
                    try { Thread.sleep(delayMs); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
        }
        throw new MainIndexException(
                "MainIndex sync failed after " + SYNC_MAX_RETRIES + " attempts for newFileId=" + newFileId,
                lastException);
    }

    // =========================================================================
    // SinglePointIndex sync
    // =========================================================================

    /**
     * Update the SinglePointIndex so that surviving rows point to their new rowIds.
     *
     * <p>For each surviving row we reconstruct its {@code IndexKey} from:
     * <ul>
     *   <li>{@code pkBytes} – the serialised primary-key column values read during the file-copy
     *       pass (same byte layout used when the row was first inserted).</li>
     *   <li>{@code writeTs} – the hidden commit timestamp stored in the file's hidden column.</li>
     * </ul>
     * The new {@code rowId} is {@code firstNewRowId + newGlobalRowId} (the same contiguous block
     * that was allocated by {@link #syncMainIndex}).
     *
     * @param group             the file group being rewritten (provides {@code tableId})
     * @param primaryIndexMeta  metadata for the table's primary SinglePointIndex
     * @param firstNewRowId     first row-ID of the contiguous block allocated for surviving rows
     * @param newRowPkBytes     per-row serialised PK bytes, in newGlobalRowId order (0-based)
     * @param newRowWriteTs     per-row write timestamps, in newGlobalRowId order (0-based)
     * @param newFileId         metadata file ID of the new file
     * @throws IndexException if the sync cannot be completed
     */
    private void syncSinglePointIndex(FileGroup group,
                                      SinglePointIndex primaryIndexMeta,
                                      long firstNewRowId,
                                      List<byte[]> newRowPkBytes,
                                      List<Long> newRowWriteTs,
                                      long newFileId) throws IndexException
    {
        long tableId = group.tableId;
        long indexId = primaryIndexMeta.getId();
        IndexOption indexOption = IndexOption.builder().vNodeId(0).build();

        int totalRows = newRowPkBytes.size();
        List<IndexProto.PrimaryIndexEntry> entries = new ArrayList<>(totalRows);
        for (int i = 0; i < totalRows; i++)
        {
            long newRowId = firstNewRowId + i;
            long writeTs  = newRowWriteTs.get(i);
            byte[] pkBytes = newRowPkBytes.get(i);

            IndexProto.IndexKey key = IndexProto.IndexKey.newBuilder()
                    .setTableId(tableId)
                    .setIndexId(indexId)
                    .setKey(ByteString.copyFrom(pkBytes))
                    .setTimestamp(writeTs)
                    .build();

            // Row location is not used by updatePrimaryEntry (it updates SPI value only;
            // MainIndex location was already set by syncMainIndex), but the proto field
            // is required so we fill a placeholder.
            IndexProto.RowLocation loc = IndexProto.RowLocation.newBuilder()
                    .setFileId(newFileId).setRgId(0).setRgRowOffset(0).build();

            entries.add(IndexProto.PrimaryIndexEntry.newBuilder()
                    .setIndexKey(key)
                    .setRowId(newRowId)
                    .setRowLocation(loc)
                    .build());
        }

        LocalIndexService.Instance().updatePrimaryIndexEntries(tableId, indexId, entries, indexOption);
        logger.info("SinglePointIndex sync complete for newFileId={}: {} entries updated", newFileId, totalRows);
    }

    // =========================================================================
    // PK serialisation helpers
    // =========================================================================

    /**
     * Resolve which schema column positions correspond to primary-key columns, and
     * return them as a two-element int[][] where:
     * <ul>
     *   <li>[0] = schema-position array (index into {@link TypeDescription#getChildren()})</li>
     *   <li>[1] = unused placeholder (category ordinals are derived later from the schema)</li>
     * </ul>
     */
    private int[][] resolvePkColumns(SinglePointIndex primaryIndexMeta, TypeDescription schema)
            throws MetadataException
    {
        List<Integer> keyColumnIds = primaryIndexMeta.getKeyColumns().getKeyColumnIds();
        long tableId = primaryIndexMeta.getTableId();
        long indexId = primaryIndexMeta.getId();
        List<Column> pkCols = IndexUtils.extractInfoFromIndex(tableId, indexId);
        List<String> fieldNames = schema.getFieldNames();

        int[] indices = new int[pkCols.size()];
        for (int pi = 0; pi < pkCols.size(); pi++)
        {
            String colName = pkCols.get(pi).getName();
            int pos = -1;
            for (int si = 0; si < fieldNames.size(); si++)
            {
                if (fieldNames.get(si).equalsIgnoreCase(colName))
                {
                    pos = si;
                    break;
                }
            }
            if (pos < 0)
            {
                throw new MetadataException("PK column '" + colName + "' not found in file schema");
            }
            indices[pi] = pos;
        }
        return new int[][]{indices, new int[0]};
    }

    /**
     * Serialise the primary-key column values for a single row into a byte array.
     *
     * <p>The byte layout matches the format used by Retina clients when they construct
     * {@code IndexKey.key}: each column value is serialised as big-endian fixed-width bytes
     * for numeric types, or raw bytes for binary/string types; the per-column bytes are
     * concatenated in the order of the primary key definition.
     *
     * @param batch            the row batch containing the row's data
     * @param rowIdx           the in-batch row index
     * @param pkColIndices     schema positions of PK columns
     * @param pkColCategories  type categories of PK columns (aligned with pkColIndices)
     * @return concatenated PK bytes for the row
     */
    private byte[] serializePkBytes(VectorizedRowBatch batch, int rowIdx,
                                    int[] pkColIndices, TypeDescription.Category[] pkColCategories)
    {
        // Fast path for single-column PK (most common case).
        if (pkColIndices.length == 1)
        {
            return serializeSingleColumn(batch.cols[pkColIndices[0]], rowIdx, pkColCategories[0]);
        }

        // Multi-column PK: accumulate parts then join.
        int totalLen = 0;
        byte[][] parts = new byte[pkColIndices.length][];
        for (int pi = 0; pi < pkColIndices.length; pi++)
        {
            parts[pi] = serializeSingleColumn(batch.cols[pkColIndices[pi]], rowIdx, pkColCategories[pi]);
            totalLen += parts[pi].length;
        }
        byte[] result = new byte[totalLen];
        int offset = 0;
        for (byte[] part : parts)
        {
            System.arraycopy(part, 0, result, offset, part.length);
            offset += part.length;
        }
        return result;
    }

    private byte[] serializeSingleColumn(ColumnVector col, int rowIdx,
                                         TypeDescription.Category category)
    {
        switch (category)
        {
            case LONG:
                return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN)
                        .putLong(((LongColumnVector) col).vector[rowIdx]).array();
            case INT:
            case DATE:
            case TIME:
                return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN)
                        .putInt((int) ((LongColumnVector) col).vector[rowIdx]).array();
            case SHORT:
                return ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN)
                        .putShort((short) ((LongColumnVector) col).vector[rowIdx]).array();
            case BYTE:
            case BOOLEAN:
                return new byte[]{(byte) ((LongColumnVector) col).vector[rowIdx]};
            case FLOAT:
                return ByteBuffer.allocate(Float.BYTES).order(ByteOrder.BIG_ENDIAN)
                        .putFloat(Float.intBitsToFloat((int) ((LongColumnVector) col).vector[rowIdx])).array();
            case DOUBLE:
                return ByteBuffer.allocate(Double.BYTES).order(ByteOrder.BIG_ENDIAN)
                        .putDouble(Double.longBitsToDouble(((LongColumnVector) col).vector[rowIdx])).array();
            case DECIMAL:
                return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN)
                        .putLong(((DecimalColumnVector) col).vector[rowIdx]).array();
            case TIMESTAMP:
            {
                TimestampColumnVector tcv = (TimestampColumnVector) col;
                return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN)
                        .putLong(tcv.times[rowIdx]).array();
            }
            case STRING:
            case VARCHAR:
            case CHAR:
            case BINARY:
            case VARBINARY:
            {
                BinaryColumnVector bcv = (BinaryColumnVector) col;
                int start = bcv.start[rowIdx];
                int len   = bcv.lens[rowIdx];
                return Arrays.copyOfRange(bcv.vector[rowIdx], start, start + len);
            }
            default:
                // Fallback: return empty bytes (should not happen for valid PK columns).
                logger.warn("Unsupported PK column category for serialisation: {}", category);
                return new byte[0];
        }
    }

    /**
     * Create a write-batch for the new file.
     *
     * <p>When {@code hasHiddenCol} is {@code true} the batch gets an extra
     * {@link LongColumnVector} at the end, which {@link PixelsWriterImpl} treats as the
     * hidden commit-timestamp column.
     */
    private VectorizedRowBatch createWriteBatch(TypeDescription schema, int batchSize, boolean hasHiddenCol)
    {
        VectorizedRowBatch batch = schema.createRowBatch(batchSize);
        if (hasHiddenCol)
        {
            ColumnVector[] extended = Arrays.copyOf(batch.cols, batch.cols.length + 1);
            extended[extended.length - 1] = new LongColumnVector(batchSize);
            batch.cols = extended;
            batch.numCols = extended.length;
        }
        return batch;
    }

    /**
     * Binary-search the cumulative array to find which new RG a given global row ID belongs to.
     *
     * @param newGlobal          the global row position in the new file (0-based)
     * @param newRgCumulativeRows cumulative[i] = total rows in RGs 0..(i-1); length = numRGs + 1
     * @return 0-based index of the new RG
     */
    private int findNewRgId(int newGlobal, int[] newRgCumulativeRows)
    {
        int lo = 0, hi = newRgCumulativeRows.length - 2;  // last valid RG index
        while (lo < hi)
        {
            int mid = (lo + hi + 1) >>> 1;
            if (newRgCumulativeRows[mid] <= newGlobal) lo = mid;
            else hi = mid - 1;
        }
        return lo;
    }

    /**
     * Build a cumulative-sum array from per-RG row counts.
     * {@code result[0] = 0}, {@code result[i+1] = result[i] + rowCounts[i]}.
     */
    private int[] buildCumulativeArray(int[] rowCounts)
    {
        int[] cumulative = new int[rowCounts.length + 1];
        for (int i = 0; i < rowCounts.length; i++)
        {
            cumulative[i + 1] = cumulative[i] + rowCounts[i];
        }
        return cumulative;
    }

    /**
     * Generate new file path for rewritten data.
     * Format: <retinaNodeId>_<timestamp>_<counter>.pxl
     */
    private String generateNewFilePath(FileGroup group)
    {
        long timestamp = System.currentTimeMillis();
        String fileName = String.format("%s_%d_0.pxl", group.retinaNodeId, timestamp);

        // Get proper directory path from metadata Path URI
        if (group.files.isEmpty()) {
            throw new IllegalStateException("File group is empty");
        }
        
        FileCandidate firstCandidate = group.files.get(0);
        Path path = firstCandidate.path;
        String dirPath = path.getUri();
        
        // Ensure path ends with '/'
        if (!dirPath.endsWith("/")) {
            dirPath += "/";
        }

        return dirPath + fileName;
    }

    /**
     * Convert RowIds in deletion blocks using the mapping.
     *
     * CRITICAL: Item encoding format (from RGVisibility.cpp:130):
     * - High 16 bits: rowId (globalRowId << 48)
     * - Low 48 bits: timestamp (timestamp & 0x0000FFFFFFFFFFFFULL)
     *
     * @param deletionBlocks raw deletion blocks from exportDeletionBlocks()
     * @param mapping rowId mapping array (mapping[oldRowId] = newRowId)
     * @return converted deletion blocks
     */
    private long[] convertDeletionBlocks(long[] deletionBlocks, int[] mapping)
    {
        List<Long> converted = new ArrayList<>();

        for (long item : deletionBlocks)
        {
            // Extract rowId and timestamp from item
            // CORRECT: High 16 bits = rowId, Low 48 bits = timestamp
            int oldRowId = (int) (item >>> 48);  // Extract high 16 bits
            long timestamp = item & 0x0000FFFFFFFFFFFFL;  // Extract low 48 bits

            // Convert rowId
            if (oldRowId < mapping.length)
            {
                int newRowId = mapping[oldRowId];
                if (newRowId >= 0)  // Not physically deleted
                {
                    // Reconstruct item with new rowId
                    // Pack: high 16 bits = newRowId, low 48 bits = timestamp
                    long newItem = (((long) newRowId << 48) | timestamp);
                    converted.add(newItem);
                }
            }
        }

        return converted.stream().mapToLong(Long::longValue).toArray();
    }

    /**
     * Calculate invalid ratio for a file by aggregating row counts across all RGs.
     * Uses accurate weighted calculation: Σ(RG invalid rows) / Σ(RG total rows),
     * rather than a simple arithmetic average of per-RG ratios.
     *
     * @param file file metadata
     * @return invalid ratio (0.0 to 1.0)
     */
    private double calculateFileInvalidRatio(File file)
    {
        try
        {
            int numRowGroups = file.getNumRowGroup();
            long totalInvalidCount = 0;
            long totalRowCount = 0;

            for (int rgId = 0; rgId < numRowGroups; rgId++)
            {
                RGVisibility rgVisibility = resourceManager.getRGVisibility(file.getId(), rgId);
                if (rgVisibility != null)
                {
                    totalInvalidCount += rgVisibility.getInvalidCount();
                    totalRowCount += rgVisibility.getRecordNum();
                }
            }

            return totalRowCount > 0 ? (double) totalInvalidCount / totalRowCount : 0.0;
        }
        catch (Exception e)
        {
            logger.warn("Failed to calculate invalid ratio for file: {}", file.getName(), e);
            return 0.0;
        }
    }

    /**
     * Get row group size from configuration or default value.
     * Priority: Configuration > Default value (256MB)
     */
    private int getRowGroupSizeFromConfig(PixelsReader reader)
    {
        try
        {
            ConfigFactory config = ConfigFactory.Instance();
            String rowGroupSizeStr = config.getProperty("row.group.size");
            if (rowGroupSizeStr != null)
            {
                return Integer.parseInt(rowGroupSizeStr);
            }
        }
        catch (Exception e)
        {
            logger.warn("Failed to get row group size from config, using default value", e);
        }
        
        // Default row group size: 256MB
        return 268435456;
    }

    /**
     * Get encoding level from configuration or default value.
     * Priority: Configuration > Default value (EL2)
     */
    private EncodingLevel getEncodingLevelFromConfig(PixelsReader reader)
    {
        try
        {
            ConfigFactory config = ConfigFactory.Instance();
            String encodingLevelStr = config.getProperty("encoding.level");
            if (encodingLevelStr != null)
            {
                return EncodingLevel.from(encodingLevelStr);
            }
        }
        catch (Exception e)
        {
            logger.warn("Failed to get encoding level from config, using default value", e);
        }
        
        // Default encoding level: EL2
        return EncodingLevel.EL2;
    }

    /**
     * Copy a single row from source batch to destination batch.
     *
     * @param src      source batch
     * @param srcIndex source row index
     * @param dst      destination batch
     * @param dstIndex destination row index
     */
    private void copyRow(VectorizedRowBatch src, int srcIndex, VectorizedRowBatch dst, int dstIndex)
    {
        for (int colIdx = 0; colIdx < src.cols.length; colIdx++)
        {
            ColumnVector srcCol = src.cols[colIdx];
            ColumnVector dstCol = dst.cols[colIdx];

            // Copy value based on column type
            if (srcCol instanceof LongColumnVector)
            {
                ((LongColumnVector) dstCol).vector[dstIndex] = ((LongColumnVector) srcCol).vector[srcIndex];
            }
            else if (srcCol instanceof DoubleColumnVector)
            {
                ((DoubleColumnVector) dstCol).vector[dstIndex] = ((DoubleColumnVector) srcCol).vector[srcIndex];
            }
            else if (srcCol instanceof BinaryColumnVector)
            {
                BinaryColumnVector srcBytes = (BinaryColumnVector) srcCol;
                BinaryColumnVector dstBytes = (BinaryColumnVector) dstCol;
                dstBytes.setVal(dstIndex, srcBytes.vector[srcIndex], srcBytes.start[srcIndex], srcBytes.lens[srcIndex]);
            }
            else if (srcCol instanceof DecimalColumnVector)
            {
                ((DecimalColumnVector) dstCol).vector[dstIndex] = ((DecimalColumnVector) srcCol).vector[srcIndex];
            }
            else if (srcCol instanceof TimestampColumnVector)
            {
                TimestampColumnVector srcTs = (TimestampColumnVector) srcCol;
                TimestampColumnVector dstTs = (TimestampColumnVector) dstCol;
                dstTs.times[dstIndex] = srcTs.times[srcIndex];
            }

            // Copy null flag
            dstCol.isNull[dstIndex] = srcCol.isNull[srcIndex];
        }
    }

    /**
     * Represents a group of files to be rewritten together.
     * Files in the same group must have:
     * - Same tableId
     * - Same retinaNodeId (to ensure timestamp monotonicity)
     */
    private static class FileGroup
    {
        final long tableId;
        final String retinaNodeId;
        final List<FileCandidate> files;

        FileGroup(long tableId, String retinaNodeId, List<FileCandidate> files)
        {
            this.tableId = tableId;
            this.retinaNodeId = retinaNodeId;
            this.files = files;
        }

        long getTotalSize()
        {
            return files.stream().mapToLong(f -> f.fileSize).sum();
        }

        @Override
        public String toString()
        {
            return String.format("FileGroup{tableId=%d, retinaNodeId=%s, fileCount=%d, totalSize=%d}",
                    tableId, retinaNodeId, files.size(), getTotalSize());
        }
    }

    /**
     * File candidate for Storage GC.
     */
    private static class FileCandidate
    {
        final File file;
        final Path path;
        final String filePath;
        final long fileSize;
        final long tableId;
        final String retinaNodeId;
        final double invalidRatio;

        FileCandidate(File file, Path path, String filePath, long fileSize,
                      long tableId, String retinaNodeId, double invalidRatio)
        {
            this.file = file;
            this.path = path;
            this.filePath = filePath;
            this.fileSize = fileSize;
            this.tableId = tableId;
            this.retinaNodeId = retinaNodeId;
            this.invalidRatio = invalidRatio;
        }
    }
}