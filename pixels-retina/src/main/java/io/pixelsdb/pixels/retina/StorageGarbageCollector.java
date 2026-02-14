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

import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import io.pixelsdb.pixels.core.writer.PixelsWriterImpl;
import io.pixelsdb.pixels.core.reader.PixelsReaderImpl;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.daemon.MetadataProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class StorageGarbageCollector
{
    private static final Logger logger = LogManager.getLogger(StorageGarbageCollector.class);
    
    private final RetinaResourceManager resourceManager;
    private final MetadataService metadataService;
    private final Storage storage;
    
    // Configuration parameters
    private final boolean enabled;
    private final double invalidRatioThreshold;
    private final long targetFileSize;
    private final int maxFilesPerRun;
    
    public StorageGarbageCollector(RetinaResourceManager resourceManager, MetadataService metadataService)
    {
        this.resourceManager = resourceManager;
        this.metadataService = metadataService;
        
        ConfigFactory config = ConfigFactory.Instance();
        this.enabled = Boolean.parseBoolean(config.getProperty("storage.gc.enabled", "true"));
        this.invalidRatioThreshold = Double.parseDouble(config.getProperty("storage.gc.threshold", "0.5"));
        this.targetFileSize = Long.parseLong(config.getProperty("storage.gc.target.file.size", "134217728"));  // 128MB
        this.maxFilesPerRun = Integer.parseInt(config.getProperty("storage.gc.max.files.per.run", "10"));
        
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
            List<MetadataProto.Schema> schemas = metadataService.getSchemas();
            Map<String, List<FileCandidate>> groupMap = new HashMap<>();
            
            for (MetadataProto.Schema schema : schemas)
            {
                List<MetadataProto.Table> tables = metadataService.getTables(schema.getName());
                
                for (MetadataProto.Table table : tables)
                {
                    // Step 2: Get all paths for this table
                    List<MetadataProto.Path> paths = metadataService.getPaths(schema.getName(), table.getName());
                    
                    for (MetadataProto.Path path : paths)
                    {
                        // Step 3: Get all files for this path
                        List<MetadataProto.File> files = metadataService.getFiles(path.getId());
                        
                        for (MetadataProto.File file : files)
                        {
                            // Step 4: Calculate invalid ratio
                            double fileInvalidRatio = calculateFileInvalidRatio(file);
                            
                            if (fileInvalidRatio > invalidRatioThreshold)
                            {
                                // Step 5: Extract retinaNodeId from file path
                                String retinaNodeId = RetinaUtils.extractRetinaNodeIdFromPath(file.getFileName());
                                
                                // Step 6: Group by (tableId, retinaNodeId)
                                String groupKey = table.getId() + "_" + retinaNodeId;
                                groupMap.computeIfAbsent(groupKey, k -> new ArrayList<>())
                                       .add(new FileCandidate(file, table.getId(), retinaNodeId, fileInvalidRatio));
                                
                                logger.debug("File {} marked for GC: invalidRatio={}", 
                                           file.getFileName(), fileInvalidRatio);
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
                List<MetadataProto.File> groupFiles = new ArrayList<>();
                long totalSize = 0;
                
                for (int i = 0; i < filesToProcess; i++)
                {
                    FileCandidate candidate = candidates.get(i);
                    groupFiles.add(candidate.file);
                    totalSize += candidate.file.getFileSize();
                    
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
        
        // Step 1: Create new file path
        String newFilePath = generateNewFilePath(group);
        logger.info("Creating new file: {}", newFilePath);
        
        // Step 2: Read first file to get schema and configuration
        MetadataProto.File firstFile = group.files.get(0);
        PixelsReader firstReader = PixelsReaderImpl.newBuilder()
                .setStorage(storage)
                .setPath(firstFile.getFilePath())
                .build();
        
        TypeDescription schema = firstReader.getFileSchema();
        long pixelStride = firstReader.getPixelStride();
        PixelsProto.CompressionKind compressionKind = firstReader.getCompressionKind();
        
        // Step 3: Create PixelsWriter for new file
        PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride((int) pixelStride)
                .setRowGroupSize(128 * 1024 * 1024)  // 128MB row group size
                .setStorage(storage)
                .setPath(newFilePath)
                .setCompressionKind(compressionKind)
                .setEncodingLevel(EncodingLevel.EL2)
                .build();
        
        // Track mappings for each old file
        Map<Long, int[]> fileMappings = new HashMap<>();
        long newFileId = System.currentTimeMillis();  // Temporary ID, will be replaced by metadata service
        int globalNewRowId = 0;
        
        // Step 4: Process each old file
        for (MetadataProto.File oldFile : group.files)
        {
            logger.info("Processing file: {}", oldFile.getFileName());
            
            PixelsReader reader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(oldFile.getFilePath())
                    .build();
            
            int numRowGroups = reader.getRowGroupNum();
            int[] fileMapping = new int[(int) reader.getNumberOfRows()];
            Arrays.fill(fileMapping, -1);  // Initialize with -1 (deleted)
            
            // Step 5: Process each row group
            for (int rgId = 0; rgId < numRowGroups; rgId++)
            {
                // Get RGVisibility and Base Bitmap
                RGVisibility rgVisibility = resourceManager.getRGVisibility(oldFile.getId(), rgId);
                if (rgVisibility == null)
                {
                    logger.warn("RGVisibility not found for fileId={}, rgId={}, skipping", oldFile.getId(), rgId);
                    continue;
                }
                
                long[] baseBitmap = rgVisibility.getBaseBitmap();
                
                // Read row group data
                PixelsReaderOption option = new PixelsReaderOption();
                option.skipCorruptRecords(true);
                option.tolerantSchemaEvolution(true);
                option.includeRGs(new int[]{rgId});
                
                PixelsRecordReader recordReader = reader.read(option);
                VectorizedRowBatch rowBatch;
                int rgRowOffset = 0;
                
                // Step 6: Filter and write data based on Base Bitmap
                while ((rowBatch = recordReader.readBatch()) != null)
                {
                    int batchSize = rowBatch.size;
                    VectorizedRowBatch filteredBatch = schema.createRowBatch(batchSize);
                    int filteredCount = 0;
                    
                    for (int i = 0; i < batchSize; i++)
                    {
                        int globalRowId = rgRowOffset + i;
                        
                        // Check Base Bitmap for deletion status
                        // Base Bitmap semantics: 1 = deleted/invalid, 0 = valid
                        // This is consistent with:
                        // - SET_BITMAP_BIT macro in TileVisibility.h:25
                        // - getInvalidCount() in TileVisibility.cpp:303
                        // - collectTileGarbage() in TileVisibility.cpp:260
                        int bitmapIndex = globalRowId / 64;
                        int bitOffset = globalRowId % 64;
                        boolean isDeleted = (baseBitmap[bitmapIndex] & (1L << bitOffset)) != 0;
                        
                        if (!isDeleted)  // Only keep valid rows (bit = 0)
                        {
                            // Copy row to filtered batch
                            copyRow(rowBatch, i, filteredBatch, filteredCount);
                            fileMapping[globalRowId] = globalNewRowId++;
                            filteredCount++;
                        }
                        else
                        {
                            fileMapping[globalRowId] = -1;  // Physically deleted
                        }
                    }
                    
                    // Write filtered batch
                    if (filteredCount > 0)
                    {
                        filteredBatch.size = filteredCount;
                        pixelsWriter.addRowBatch(filteredBatch);
                    }
                    
                    rgRowOffset += batchSize;
                }
                
                recordReader.close();
            }
            
            fileMappings.put(oldFile.getId(), fileMapping);
            reader.close();
        }
        
        // Step 7: Close writer and get new file metadata
        pixelsWriter.close();
        
        // Step 8: Migrate Deletion Chain
        // CRITICAL: This must be done BEFORE registering dual-write mapping
        // to ensure new file's Visibility is ready before any concurrent deletes
        Map<Integer, long[]> newRGDeletions = new HashMap<>();
        
        for (MetadataProto.File oldFile : group.files)
        {
            int[] mapping = fileMappings.get(oldFile.getId());
            int numRowGroups = (int) (oldFile.getNumRowGroup());
            
            for (int rgId = 0; rgId < numRowGroups; rgId++)
            {
                RGVisibility oldRgVisibility = resourceManager.getRGVisibility(oldFile.getId(), rgId);
                if (oldRgVisibility == null) continue;
                
                // Export Deletion Chain from old file
                long[] deletionBlocks = oldRgVisibility.exportDeletionBlocks();
                
                // Convert RowIds using mapping
                long[] convertedBlocks = convertDeletionBlocks(deletionBlocks, mapping);
                
                if (convertedBlocks.length > 0)
                {
                    // Accumulate deletions for each new RG
                    // TODO: Determine correct new RG ID based on row distribution
                    int newRgId = rgId;  // Simplified: assume same RG structure
                    long[] existing = newRGDeletions.getOrDefault(newRgId, new long[0]);
                    long[] merged = new long[existing.length + convertedBlocks.length];
                    System.arraycopy(existing, 0, merged, 0, existing.length);
                    System.arraycopy(convertedBlocks, 0, merged, existing.length, convertedBlocks.length);
                    newRGDeletions.put(newRgId, merged);
                    
                    logger.info("Exported {} deletion blocks from oldFile={}, rgId={}", 
                               convertedBlocks.length, oldFile.getId(), rgId);
                }
            }
        }
        
        // Create RGVisibility for new file and prepend deletion blocks
        // This must happen BEFORE registering dual-write mapping
        for (Map.Entry<Integer, long[]> entry : newRGDeletions.entrySet())
        {
            int newRgId = entry.getKey();
            long[] deletions = entry.getValue();
            
            // Create new RGVisibility instance
            // TODO: Get proper parameters (numTiles, capacity) from new file metadata
            RGVisibility newRgVisibility = resourceManager.createRGVisibility(
                newFileId, newRgId, 1024, 65536);  // Placeholder values
            
            // Prepend deletion blocks to new file
            newRgVisibility.prependDeletionBlocks(deletions);
            
            logger.info("Prepended {} deletion blocks to newFile={}, rgId={}", 
                       deletions.length, newFileId, newRgId);
        }
        
        // Step 9: Register dual-write mapping
        for (Map.Entry<Long, int[]> entry : fileMappings.entrySet())
        {
            resourceManager.registerRedirection(entry.getKey(), newFileId, entry.getValue());
        }
        
        // Step 10: Atomic swap metadata
        List<MetadataProto.File> filesToAdd = Arrays.asList(
                MetadataProto.File.newBuilder()
                        .setFileName(newFilePath.substring(newFilePath.lastIndexOf('/') + 1))
                        .setFilePath(newFilePath)
                        .setFileSize(storage.getFileLength(newFilePath))
                        .setNumRowGroup(pixelsWriter.getNumRowGroup())
                        .build()
        );
        
        List<Long> filesToDelete = group.files.stream()
                .map(MetadataProto.File::getId)
                .collect(Collectors.toList());
        
        boolean swapSuccess = metadataService.atomicSwapFiles(filesToAdd, filesToDelete);
        
        if (swapSuccess)
        {
            logger.info("Metadata swap succeeded for file group: {}", group);
            
            // Step 11: Unregister mapping
            for (Long oldFileId : filesToDelete)
            {
                resourceManager.unregisterRedirection(oldFileId, newFileId);
            }
        }
        else
        {
            logger.error("Metadata swap failed for file group: {}, rolling back", group);
            
            // Rollback: Delete new file and unregister mappings
            storage.delete(newFilePath);
            for (Long oldFileId : filesToDelete)
            {
                resourceManager.unregisterRedirection(oldFileId, newFileId);
            }
            
            throw new Exception("Metadata swap failed");
        }
    }
    
    /**
     * Generate new file path for rewritten data.
     * Format: <retinaNodeId>_<timestamp>_<counter>.pxl
     */
    private String generateNewFilePath(FileGroup group)
    {
        long timestamp = System.currentTimeMillis();
        String fileName = String.format("%s_%d_0.pxl", group.retinaNodeId, timestamp);
        
        // TODO: Get proper directory path from metadata
        String dirPath = "/tmp/pixels/";
        
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
            int oldRowId = (int)(item >>> 48);  // Extract high 16 bits
            long timestamp = item & 0x0000FFFFFFFFFFFFL;  // Extract low 48 bits
            
            // Convert rowId
            if (oldRowId < mapping.length)
            {
                int newRowId = mapping[oldRowId];
                if (newRowId >= 0)  // Not physically deleted
                {
                    // Reconstruct item with new rowId
                    // Pack: high 16 bits = newRowId, low 48 bits = timestamp
                    long newItem = (((long)newRowId << 48) | timestamp);
                    converted.add(newItem);
                }
            }
        }
        
        return converted.stream().mapToLong(Long::longValue).toArray();
    }
    
    /**
     * Calculate invalid ratio for a file by aggregating all RG invalid ratios.
     * 
     * @param file file metadata
     * @return invalid ratio (0.0 to 1.0)
     */
    private double calculateFileInvalidRatio(MetadataProto.File file)
    {
        try
        {
            int numRowGroups = (int) file.getNumRowGroup();
            double totalInvalidRatio = 0.0;
            int validRGCount = 0;
            
            for (int rgId = 0; rgId < numRowGroups; rgId++)
            {
                RGVisibility rgVisibility = resourceManager.getRGVisibility(file.getId(), rgId);
                if (rgVisibility != null)
                {
                    totalInvalidRatio += rgVisibility.getInvalidRatio();
                    validRGCount++;
                }
            }
            
            return validRGCount > 0 ? totalInvalidRatio / validRGCount : 0.0;
        }
        catch (Exception e)
        {
            logger.warn(\"Failed to calculate invalid ratio for file: {}\", file.getFileName(), e);
            return 0.0;
        }
    }
    
    /**
     * Copy a single row from source batch to destination batch.
     * 
     * @param src source batch
     * @param srcIndex source row index
     * @param dst destination batch
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
            else if (srcCol instanceof BytesColumnVector)
            {
                BytesColumnVector srcBytes = (BytesColumnVector) srcCol;
                BytesColumnVector dstBytes = (BytesColumnVector) dstCol;
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
                dstTs.nanos[dstIndex] = srcTs.nanos[srcIndex];
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
        final List<MetadataProto.File> files;
        
        FileGroup(long tableId, String retinaNodeId, List<MetadataProto.File> files)
        {
            this.tableId = tableId;
            this.retinaNodeId = retinaNodeId;
            this.files = files;
        }
        
        long getTotalSize()
        {
            return files.stream().mapToLong(f -> f.getFileSize()).sum();
        }
        
        @Override
        public String toString()
        {
            return String.format(\"FileGroup{tableId=%d, retinaNodeId=%s, fileCount=%d, totalSize=%d}\", 
                               tableId, retinaNodeId, files.size(), getTotalSize());
        }
    }
    
    /**
     * File candidate for Storage GC.
     */
    private static class FileCandidate
    {
        final MetadataProto.File file;
        final long tableId;
        final String retinaNodeId;
        final double invalidRatio;
        
        FileCandidate(MetadataProto.File file, long tableId, String retinaNodeId, double invalidRatio)
        {
            this.file = file;
            this.tableId = tableId;
            this.retinaNodeId = retinaNodeId;
            this.invalidRatio = invalidRatio;
        }
    }
}