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
 * Test validator for Storage GC test results.
 * Validates data consistency, mapping correctness, and visibility synchronization.
 */
package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;

import java.util.*;

/**
 * Validates Storage GC test results for correctness and consistency.
 */
public class StorageGCTestValidator
{
    // ============================
    // File Grouping Validation
    // ============================
    
    /**
     * Validate file grouping by (tableId, retinaNodeId) constraints.
     */
    public static ValidationResult validateFileGrouping(List<File> files, List<List<File>> groups)
    {
        ValidationResult result = new ValidationResult("File Grouping Validation");
        
        // Check that all files are assigned to groups
        Set<Long> groupedFileIds = new HashSet<>();
        for (List<File> group : groups)
        {
            for (File file : group)
            {
                groupedFileIds.add(file.getId());
            }
        }
        
        for (File file : files)
        {
            if (!groupedFileIds.contains(file.getId()))
            {
                result.addError("File " + file.getId() + " not assigned to any group");
            }
        }
        
        // Validate grouping constraints
        for (List<File> group : groups)
        {
            if (group.isEmpty())
            {
                result.addError("Empty group found");
                continue;
            }
            
            // Check that all files in group have same tableId and retinaNodeId
            long firstTableId = group.get(0).getTableId();
            String firstNodeId = group.get(0).getRetinaNodeId();
            
            for (int i = 1; i < group.size(); i++)
            {
                File file = group.get(i);
                if (file.getTableId() != firstTableId)
                {
                    result.addError("Group contains files from different tables: " + 
                                   firstTableId + " vs " + file.getTableId());
                }
                if (!file.getRetinaNodeId().equals(firstNodeId))
                {
                    result.addError("Group contains files from different nodes: " + 
                                   firstNodeId + " vs " + file.getRetinaNodeId());
                }
            }
        }
        
        return result;
    }
    
    // ============================
    // Invalid Ratio Calculation Validation
    // ============================
    
    /**
     * Validate invalid ratio calculation accuracy.
     */
    public static ValidationResult validateInvalidRatioCalculation(
        RGVisibility rgVisibility, double calculatedRatio, double expectedRatio, double tolerance)
    {
        ValidationResult result = new ValidationResult("Invalid Ratio Calculation Validation");
        
        if (rgVisibility == null)
        {
            result.addError("RGVisibility is null");
            return result;
        }
        
        long totalRows = file.getNumRows(); // Use file's actual row count instead of RG-level count
        long invalidRows = rgVisibility.getInvalidCount();
        
        if (totalRows <= 0)
        {
            result.addError("Total row count must be positive: " + totalRows);
            return result;
        }
        
        // Calculate actual ratio
        double actualRatio = (double) invalidRows / totalRows;
        
        // Check calculation accuracy
        if (Math.abs(calculatedRatio - actualRatio) > tolerance)
        {
            result.addError(String.format(
                "Invalid ratio calculation error: expected %.4f, actual %.4f, calculated %.4f",
                expectedRatio, actualRatio, calculatedRatio));
        }
        
        // Check against expected ratio
        if (Math.abs(actualRatio - expectedRatio) > tolerance)
        {
            result.addError(String.format(
                "Invalid ratio mismatch: expected %.4f, actual %.4f",
                expectedRatio, actualRatio));
        }
        
        // Validate ratio bounds
        if (actualRatio < 0.0 || actualRatio > 1.0)
        {
            result.addError("Invalid ratio out of bounds [0,1]: " + actualRatio);
        }
        
        return result;
    }
    
    // ============================
    // RowId Mapping Validation
    // ============================
    
    /**
     * Validate RowId mapping consistency and correctness.
     */
    public static ValidationResult validateRowIdMapping(int[] mapping, int totalOldRows, int expectedNewRows)
    {
        ValidationResult result = new ValidationResult("RowId Mapping Validation");
        
        if (mapping == null)
        {
            result.addError("Mapping array is null");
            return result;
        }
        
        if (mapping.length != totalOldRows)
        {
            result.addError(String.format(
                "Mapping length mismatch: expected %d, actual %d",
                totalOldRows, mapping.length));
        }
        
        // Count valid mappings and check for duplicates
        Set<Integer> newRowIds = new HashSet<>();
        int validMappings = 0;
        
        for (int oldRowId = 0; oldRowId < mapping.length; oldRowId++)
        {
            int newRowId = mapping[oldRowId];
            
            if (newRowId >= 0)
            {
                validMappings++;
                
                // Check for duplicate new row IDs
                if (newRowIds.contains(newRowId))
                {
                    result.addError("Duplicate new row ID: " + newRowId);
                }
                newRowIds.add(newRowId);
                
                // Validate new row ID bounds
                if (newRowId >= expectedNewRows)
                {
                    result.addError(String.format(
                        "New row ID out of bounds: %d >= %d", newRowId, expectedNewRows));
                }
            }
            else if (newRowId != -1)
            {
                result.addError("Invalid mapping value: " + newRowId + " (should be -1 for deleted rows)");
            }
        }
        
        // Validate expected new row count
        if (validMappings != expectedNewRows)
        {
            result.addError(String.format(
                "New row count mismatch: expected %d, actual %d",
                expectedNewRows, validMappings));
        }
        
        return result;
    }
    
    // ============================
    // Deletion Chain Validation
    // ============================
    
    /**
     * Validate deletion chain consistency after conversion.
     */
    public static ValidationResult validateDeletionChainConversion(
        long[] originalDeletionChain, long[] convertedDeletionChain, int[] mapping)
    {
        ValidationResult result = new ValidationResult("Deletion Chain Conversion Validation");
        
        if (originalDeletionChain == null && convertedDeletionChain == null)
        {
            return result; // Both null is valid
        }
        
        if (originalDeletionChain == null || convertedDeletionChain == null)
        {
            result.addError("One deletion chain is null while the other is not");
            return result;
        }
        
        // Count valid deletions after conversion
        int validConvertedDeletions = 0;
        
        for (long deletionItem : originalDeletionChain)
        {
            int oldRowId = (int) (deletionItem >>> 48);
            long timestamp = deletionItem & 0x0000FFFFFFFFFFFFL;
            
            if (oldRowId < mapping.length)
            {
                int newRowId = mapping[oldRowId];
                if (newRowId >= 0)
                {
                    validConvertedDeletions++;
                }
            }
        }
        
        if (validConvertedDeletions != convertedDeletionChain.length)
        {
            result.addError(String.format(
                "Converted deletion chain length mismatch: expected %d, actual %d",
                validConvertedDeletions, convertedDeletionChain.length));
        }
        
        // Validate converted deletion items
        for (long convertedItem : convertedDeletionChain)
        {
            int newRowId = (int) (convertedItem >>> 48);
            long timestamp = convertedItem & 0x0000FFFFFFFFFFFFL;
            
            // Check that new row ID is valid
            if (newRowId < 0)
            {
                result.addError("Invalid new row ID in converted deletion chain: " + newRowId);
            }
            
            // Check timestamp is reasonable (within last year)
            long currentTime = System.currentTimeMillis();
            long oneYearAgo = currentTime - 365L * 24 * 60 * 60 * 1000;
            
            if (timestamp < oneYearAgo || timestamp > currentTime)
            {
                result.addError("Unreasonable timestamp in deletion chain: " + timestamp);
            }
        }
        
        return result;
    }
    
    // ============================
    // Atomic Swap Validation
    // ============================
    
    /**
     * Validate atomic swap operation consistency.
     */
    public static ValidationResult validateAtomicSwap(
        List<File> oldFiles, List<File> newFiles, boolean swapResult)
    {
        ValidationResult result = new ValidationResult("Atomic Swap Validation");
        
        if (oldFiles == null || newFiles == null)
        {
            result.addError("File lists cannot be null");
            return result;
        }
        
        if (oldFiles.size() != newFiles.size())
        {
            result.addError(String.format(
                "File list size mismatch: old=%d, new=%d",
                oldFiles.size(), newFiles.size()));
        }
        
        // Validate swap result consistency
        if (swapResult)
        {
            // Successful swap: files should be properly replaced
            for (int i = 0; i < oldFiles.size(); i++)
            {
                File oldFile = oldFiles.get(i);
                File newFile = newFiles.get(i);
                
                // Check that new file has same table and node
                if (oldFile.getTableId() != newFile.getTableId())
                {
                    result.addError("Table ID mismatch after swap");
                }
                if (!oldFile.getRetinaNodeId().equals(newFile.getRetinaNodeId()))
                {
                    result.addError("Retina node ID mismatch after swap");
                }
            }
        }
        else
        {
            // Failed swap: no changes should be committed
            // This is more difficult to validate externally
            result.addInfo("Atomic swap failed - rollback expected");
        }
        
        return result;
    }
    
    // ============================
    // Dual-write Mapping Validation
    // ============================
    
    /**
     * Validate dual-write mapping consistency.
     */
    public static ValidationResult validateDualWriteMapping(
        long oldFileId, long newFileId, int[] forwardMapping, int[] backwardMapping)
    {
        ValidationResult result = new ValidationResult("Dual-write Mapping Validation");
        
        if (forwardMapping == null || backwardMapping == null)
        {
            result.addError("Mapping arrays cannot be null");
            return result;
        }
        
        // Validate forward mapping consistency
        for (int oldRowId = 0; oldRowId < forwardMapping.length; oldRowId++)
        {
            int newRowId = forwardMapping[oldRowId];
            
            if (newRowId >= 0)
            {
                // Check backward mapping consistency
                if (newRowId >= backwardMapping.length)
                {
                    result.addError(String.format(
                        "Backward mapping index out of bounds: %d >= %d",
                        newRowId, backwardMapping.length));
                    continue;
                }
                
                int backMappedRowId = backwardMapping[newRowId];
                if (backMappedRowId != oldRowId)
                {
                    result.addError(String.format(
                        "Mapping inconsistency: forward[%d]=%d, backward[%d]=%d",
                        oldRowId, newRowId, newRowId, backMappedRowId));
                }
            }
        }
        
        // Validate backward mapping consistency
        for (int newRowId = 0; newRowId < backwardMapping.length; newRowId++)
        {
            int oldRowId = backwardMapping[newRowId];
            
            if (oldRowId >= 0)
            {
                if (oldRowId >= forwardMapping.length)
                {
                    result.addError(String.format(
                        "Forward mapping index out of bounds: %d >= %d",
                        oldRowId, forwardMapping.length));
                    continue;
                }
                
                int forwardMappedRowId = forwardMapping[oldRowId];
                if (forwardMappedRowId != newRowId)
                {
                    result.addError(String.format(
                        "Mapping inconsistency: backward[%d]=%d, forward[%d]=%d",
                        newRowId, oldRowId, oldRowId, forwardMappedRowId));
                }
            }
        }
        
        return result;
    }
    
    // ============================
    // Performance Validation
    // ============================
    
    /**
     * Validate performance metrics against thresholds.
     */
    public static ValidationResult validatePerformanceMetrics(
        PerformanceMetrics metrics, PerformanceThresholds thresholds)
    {
        ValidationResult result = new ValidationResult("Performance Metrics Validation");
        
        if (metrics == null || thresholds == null)
        {
            result.addError("Metrics or thresholds cannot be null");
            return result;
        }
        
        // Validate memory usage
        if (metrics.getMaxMemoryUsage() > thresholds.getMaxMemoryMB() * 1024 * 1024)
        {
            result.addError(String.format(
                "Memory usage exceeded threshold: %.2f MB > %.2f MB",
                metrics.getMaxMemoryUsage() / (1024.0 * 1024.0),
                thresholds.getMaxMemoryMB()));
        }
        
        // Validate execution time
        if (metrics.getExecutionTimeMs() > thresholds.getMaxExecutionTimeMs())
        {
            result.addError(String.format(
                "Execution time exceeded threshold: %d ms > %d ms",
                metrics.getExecutionTimeMs(), thresholds.getMaxExecutionTimeMs()));
        }
        
        // Validate throughput
        double actualThroughput = metrics.getProcessedRows() / (metrics.getExecutionTimeMs() / 1000.0);
        if (actualThroughput < thresholds.getMinThroughputRowsPerSec())
        {
            result.addError(String.format(
                "Throughput below threshold: %.0f rows/sec < %.0f rows/sec",
                actualThroughput, thresholds.getMinThroughputRowsPerSec()));
        }
        
        return result;
    }
    
    // ============================
    // Data Classes
    // ============================
    
    public static class ValidationResult
    {
        private final String validationName;
        private final List<String> errors;
        private final List<String> warnings;
        private final List<String> info;
        
        public ValidationResult(String validationName)
        {
            this.validationName = validationName;
            this.errors = new ArrayList<>();
            this.warnings = new ArrayList<>();
            this.info = new ArrayList<>();
        }
        
        public void addError(String error)
        {
            errors.add(error);
        }
        
        public void addWarning(String warning)
        {
            warnings.add(warning);
        }
        
        public void addInfo(String infoMessage)
        {
            info.add(infoMessage);
        }
        
        public boolean isValid()
        {
            return errors.isEmpty();
        }
        
        public boolean hasWarnings()
        {
            return !warnings.isEmpty();
        }
        
        public String getSummary()
        {
            return String.format("%s: %d errors, %d warnings, %d info",
                               validationName, errors.size(), warnings.size(), info.size());
        }
        
        public String getDetailedReport()
        {
            StringBuilder report = new StringBuilder();
            report.append("=== Validation Report: ").append(validationName).append(" ===\n");
            
            if (!errors.isEmpty())
            {
                report.append("Errors:\n");
                for (String error : errors)
                {
                    report.append("  - ").append(error).append("\n");
                }
            }
            
            if (!warnings.isEmpty())
            {
                report.append("Warnings:\n");
                for (String warning : warnings)
                {
                    report.append("  - ").append(warning).append("\n");
                }
            }
            
            if (!info.isEmpty())
            {
                report.append("Info:\n");
                for (String infoMsg : info)
                {
                    report.append("  - ").append(infoMsg).append("\n");
                }
            }
            
            return report.toString();
        }
    }
    
    public static class PerformanceMetrics
    {
        private final long maxMemoryUsage;
        private final long executionTimeMs;
        private final long processedRows;
        private final int processedFiles;
        
        public PerformanceMetrics(long maxMemoryUsage, long executionTimeMs, 
                                long processedRows, int processedFiles)
        {
            this.maxMemoryUsage = maxMemoryUsage;
            this.executionTimeMs = executionTimeMs;
            this.processedRows = processedRows;
            this.processedFiles = processedFiles;
        }
        
        public long getMaxMemoryUsage() { return maxMemoryUsage; }
        public long getExecutionTimeMs() { return executionTimeMs; }
        public long getProcessedRows() { return processedRows; }
        public int getProcessedFiles() { return processedFiles; }
    }
    
    public static class PerformanceThresholds
    {
        private final double maxMemoryMB;
        private final long maxExecutionTimeMs;
        private final double minThroughputRowsPerSec;
        
        public PerformanceThresholds(double maxMemoryMB, long maxExecutionTimeMs, 
                                     double minThroughputRowsPerSec)
        {
            this.maxMemoryMB = maxMemoryMB;
            this.maxExecutionTimeMs = maxExecutionTimeMs;
            this.minThroughputRowsPerSec = minThroughputRowsPerSec;
        }
        
        public double getMaxMemoryMB() { return maxMemoryMB; }
        public long getMaxExecutionTimeMs() { return maxExecutionTimeMs; }
        public double getMinThroughputRowsPerSec() { return minThroughputRowsPerSec; }
    }
}