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
 * Test data generator for Storage GC unit tests.
 * Generates mock files, RGVisibility data, and test scenarios.
 */
package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates test data for Storage GC unit tests.
 */
public class StorageGCTestDataGenerator
{
    private static final Random random = new Random(42); // Fixed seed for reproducibility
    
    // ============================
    // File Generation Methods
    // ============================
    
    /**
     * Generate a mock File object with specified properties.
     */
    public static File generateMockFile(long fileId, String fileName, int tableId, 
                                       String retinaNodeId, long fileSize, int rowCount)
    {
        File file = new File();
        file.setId(fileId);
        file.setName(fileName);
        file.setTableId(tableId);
        file.setRetinaNodeId(retinaNodeId);
        file.setSize(fileSize);
        file.setNumRowGroup(calculateRowGroupCount(rowCount));
        file.setCreatedTime(System.currentTimeMillis());
        file.setLastModifiedTime(System.currentTimeMillis());
        return file;
    }
    
    /**
     * Generate a list of files for testing file grouping logic.
     */
    public static List<File> generateFileGroupForTesting(int groupSize, int tableId, 
                                                        String retinaNodeId, 
                                                        double invalidRatioRangeMin,
                                                        double invalidRatioRangeMax)
    {
        List<File> files = new ArrayList<>();
        
        for (int i = 0; i < groupSize; i++)
        {
            long fileId = 1000L + i;
            String fileName = String.format("table%d_node%s_file%d.pxl", 
                                           tableId, retinaNodeId, i);
            long fileSize = generateFileSize(1048576L, 10485760L); // 1MB to 10MB
            int rowCount = generateRowCount(1000, 100000);
            
            File file = generateMockFile(fileId, fileName, tableId, retinaNodeId, fileSize, rowCount);
            files.add(file);
        }
        
        return files;
    }
    
    /**
     * Generate files with different table/node combinations to test grouping constraints.
     */
    public static List<File> generateCrossTableFilesForTesting()
    {
        List<File> files = new ArrayList<>();
        
        // Files from table1, node1
        files.add(generateMockFile(1001L, "table1_node1_file1.pxl", 1, "node1", 5242880L, 50000));
        files.add(generateMockFile(1002L, "table1_node1_file2.pxl", 1, "node1", 6291456L, 60000));
        
        // Files from table2, node1 (different table, same node)
        files.add(generateMockFile(1003L, "table2_node1_file1.pxl", 2, "node1", 7340032L, 70000));
        
        // Files from table1, node2 (same table, different node)
        files.add(generateMockFile(1004L, "table1_node2_file1.pxl", 1, "node2", 8388608L, 80000));
        
        return files;
    }
    
    // ============================
    // RGVisibility Data Generation
    // ============================
    
    /**
     * Generate mock RGVisibility data with specified invalid ratio.
     */
    public static RGVisibility generateMockRGVisibility(long fileId, int rgId, 
                                                        int totalRowCount, double invalidRatio)
    {
        RGVisibility rgVisibility = new RGVisibility();
        
        long invalidCount = (long) (totalRowCount * invalidRatio);
        long validCount = totalRowCount - invalidCount;
        
        // Generate BaseBitmap (simplified representation)
        long[] baseBitmap = generateBaseBitmap(totalRowCount, invalidRatio);
        
        // Generate DeletionChain
        long[] deletionChain = generateDeletionChain(invalidCount, totalRowCount);
        
        rgVisibility.setFileId(fileId);
        rgVisibility.setRowGroupId(rgId);
        rgVisibility.setTotalRowCount(totalRowCount);
        rgVisibility.setInvalidCount(invalidCount);
        rgVisibility.setBaseBitmap(baseBitmap);
        rgVisibility.setDeletionChain(deletionChain);
        
        return rgVisibility;
    }
    
    /**
     * Generate BaseBitmap array for testing filtering logic.
     */
    public static long[] generateBaseBitmap(int totalRowCount, double invalidRatio)
    {
        int bitmapSize = (totalRowCount + 63) / 64;
        long[] baseBitmap = new long[bitmapSize];
        
        for (int i = 0; i < totalRowCount; i++)
        {
            boolean isInvalid = random.nextDouble() < invalidRatio;
            if (isInvalid)
            {
                int bitmapIndex = i / 64;
                int bitOffset = i % 64;
                baseBitmap[bitmapIndex] |= (1L << bitOffset);
            }
        }
        
        return baseBitmap;
    }
    
    /**
     * Generate DeletionChain for testing visibility synchronization.
     */
    public static long[] generateDeletionChain(long invalidCount, int totalRowCount)
    {
        if (invalidCount == 0)
        {
            return new long[0];
        }
        
        long[] deletionChain = new long[(int) invalidCount];
        Set<Integer> deletedRows = new HashSet<>();
        
        for (int i = 0; i < invalidCount; i++)
        {
            int rowId;
            do
            {
                rowId = random.nextInt(totalRowCount);
            } while (deletedRows.contains(rowId));
            
            deletedRows.add(rowId);
            
            long timestamp = System.currentTimeMillis() - random.nextInt(86400000); // Within 24 hours
            deletionChain[i] = ((long) rowId << 48) | (timestamp & 0x0000FFFFFFFFFFFFL);
        }
        
        return deletionChain;
    }
    
    // ============================
    // RowId Mapping Generation
    // ============================
    
    /**
     * Generate RowId mapping array for testing data rewriting.
     */
    public static int[] generateRowIdMapping(int totalRowCount, double invalidRatio)
    {
        int[] mapping = new int[totalRowCount];
        Arrays.fill(mapping, -1); // Initialize all as deleted
        
        int newRowId = 0;
        for (int oldRowId = 0; oldRowId < totalRowCount; oldRowId++)
        {
            // Simulate BaseBitmap filtering: keep row with probability (1 - invalidRatio)
            boolean keepRow = random.nextDouble() > invalidRatio;
            
            if (keepRow)
            {
                mapping[oldRowId] = newRowId;
                newRowId++;
            }
        }
        
        return mapping;
    }
    
    /**
     * Generate a mapping where all rows are preserved (no deletions).
     */
    public static int[] generateFullPreservationMapping(int totalRowCount)
    {
        int[] mapping = new int[totalRowCount];
        for (int i = 0; i < totalRowCount; i++)
        {
            mapping[i] = i;
        }
        return mapping;
    }
    
    /**
     * Generate a mapping where all rows are deleted (empty file).
     */
    public static int[] generateEmptyFileMapping(int totalRowCount)
    {
        int[] mapping = new int[totalRowCount];
        Arrays.fill(mapping, -1);
        return mapping;
    }
    
    // ============================
    // Test Scenario Generation
    // ============================
    
    /**
     * Generate test scenario for normal trigger (invalid ratio > threshold).
     */
    public static TestScenario generateNormalTriggerScenario()
    {
        TestScenario scenario = new TestScenario();
        scenario.setName("Normal Trigger - Invalid Ratio > 0.5");
        scenario.setFile(generateMockFile(2001L, "high_invalid_file.pxl", 1, "node1", 
                                        8388608L, 100000));
        scenario.setInvalidRatio(0.7);
        scenario.setExpectedAction("MARK_FOR_GC");
        return scenario;
    }
    
    /**
     * Generate test scenario for boundary trigger (invalid ratio = threshold).
     */
    public static TestScenario generateBoundaryTriggerScenario()
    {
        TestScenario scenario = new TestScenario();
        scenario.setName("Boundary Trigger - Invalid Ratio = 0.5");
        scenario.setFile(generateMockFile(2002L, "boundary_file.pxl", 1, "node1", 
                                        6291456L, 80000));
        scenario.setInvalidRatio(0.5);
        scenario.setExpectedAction("MARK_FOR_GC");
        return scenario;
    }
    
    /**
     * Generate test scenario for no trigger (invalid ratio < threshold).
     */
    public static TestScenario generateNoTriggerScenario()
    {
        TestScenario scenario = new TestScenario();
        scenario.setName("No Trigger - Invalid Ratio < 0.5");
        scenario.setFile(generateMockFile(2003L, "low_invalid_file.pxl", 1, "node1", 
                                        5242880L, 60000));
        scenario.setInvalidRatio(0.3);
        scenario.setExpectedAction("SKIP");
        return scenario;
    }
    
    /**
     * Generate performance test scenario with large files.
     */
    public static TestScenario generatePerformanceTestScenario()
    {
        TestScenario scenario = new TestScenario();
        scenario.setName("Performance Test - Large File");
        scenario.setFile(generateMockFile(3001L, "large_file.pxl", 1, "node1", 
                                        134217728L, 1000000)); // 128MB, 1M rows
        scenario.setInvalidRatio(0.6);
        scenario.setExpectedAction("MARK_FOR_GC");
        return scenario;
    }
    
    // ============================
    // Helper Methods
    // ============================
    
    private static int calculateRowGroupCount(int rowCount)
    {
        int defaultRowGroupSize = 65536;
        return (rowCount + defaultRowGroupSize - 1) / defaultRowGroupSize;
    }
    
    private static long generateFileSize(long minSize, long maxSize)
    {
        return minSize + (long) (random.nextDouble() * (maxSize - minSize));
    }
    
    private static int generateRowCount(int minCount, int maxCount)
    {
        return minCount + random.nextInt(maxCount - minCount + 1);
    }
    
    // ============================
    // Test Scenario Data Class
    // ============================
    
    public static class TestScenario
    {
        private String name;
        private File file;
        private double invalidRatio;
        private String expectedAction;
        private List<RGVisibility> rgVisibilities;
        
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        
        public File getFile() { return file; }
        public void setFile(File file) { this.file = file; }
        
        public double getInvalidRatio() { return invalidRatio; }
        public void setInvalidRatio(double invalidRatio) { this.invalidRatio = invalidRatio; }
        
        public String getExpectedAction() { return expectedAction; }
        public void setExpectedAction(String expectedAction) { this.expectedAction = expectedAction; }
        
        public List<RGVisibility> getRgVisibilities() { return rgVisibilities; }
        public void setRgVisibilities(List<RGVisibility> rgVisibilities) { this.rgVisibilities = rgVisibilities; }
    }
}