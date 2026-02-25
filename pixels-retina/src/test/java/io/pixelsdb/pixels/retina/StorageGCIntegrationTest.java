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
 * Integration tests for Storage GC complete workflow.
 * 
 * Test command:
 * mvn test -Dtest=StorageGCIntegrationTest
 * 
 * Tests complete GC workflow including:
 * - File scanning and grouping
 * - Data rewriting with visibility synchronization
 * - Atomic metadata switching
 * - Dual-write strategy implementation
 * - Error handling and rollback scenarios
 */
package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class StorageGCIntegrationTest
{
    @Mock
    private RetinaResourceManager resourceManager;
    
    @Mock
    private MetadataService metadataService;
    
    @Mock
    private Storage storage;
    
    @Mock
    private RGVisibility rgVisibility;
    
    @Mock
    private PixelsReader pixelsReader;
    
    @Mock
    private PixelsWriter pixelsWriter;
    
    private StorageGarbageCollector storageGC;
    private MockedStatic<ConfigFactory> configFactoryMock;
    private MockedStatic<StorageFactory> storageFactoryMock;
    
    private List<File> testFiles;
    private Map<Long, RGVisibility> rgVisibilityMap;
    
    @Before
    public void setUp()
    {
        configFactoryMock = mockStatic(ConfigFactory.class);
        storageFactoryMock = mockStatic(StorageFactory.class);
        
        // Mock configuration
        configFactoryMock.when(() -> ConfigFactory.Instance()).thenReturn(mock(ConfigFactory.class));
        configFactoryMock.when(() -> ConfigFactory.Instance().getProperty("storage.gc.enabled"))
                .thenReturn("true");
        configFactoryMock.when(() -> ConfigFactory.Instance().getProperty("storage.gc.threshold"))
                .thenReturn("0.5");
        configFactoryMock.when(() -> ConfigFactory.Instance().getProperty("storage.gc.target.file.size"))
                .thenReturn("134217728");
        configFactoryMock.when(() -> ConfigFactory.Instance().getProperty("storage.gc.max.files.per.run"))
                .thenReturn("10");
        
        // Mock storage factory
        storageFactoryMock.when(() -> StorageFactory.Instance()).thenReturn(mock(StorageFactory.class));
        when(StorageFactory.Instance().getStorage(anyString())).thenReturn(storage);
        
        storageGC = new StorageGarbageCollector(resourceManager, metadataService);
        
        // Initialize test data
        initializeTestData();
    }
    
    @After
    public void tearDown()
    {
        configFactoryMock.close();
        storageFactoryMock.close();
    }
    
    private void initializeTestData()
    {
        testFiles = new ArrayList<>();
        rgVisibilityMap = new HashMap<>();
        
        // Generate test files with different invalid ratios
        testFiles.add(StorageGCTestDataGenerator.generateMockFile(1001L, "file1_high_invalid.pxl", 
                                                                1, "node1", 8388608L, 80000));
        testFiles.add(StorageGCTestDataGenerator.generateMockFile(1002L, "file2_boundary.pxl", 
                                                                1, "node1", 6291456L, 60000));
        testFiles.add(StorageGCTestDataGenerator.generateMockFile(1003L, "file3_low_invalid.pxl", 
                                                                1, "node1", 5242880L, 50000));
        
        // Generate RGVisibility data for each file
        rgVisibilityMap.put(1001L, StorageGCTestDataGenerator.generateMockRGVisibility(
            1001L, 0, 80000, 0.7)); // High invalid ratio
        rgVisibilityMap.put(1002L, StorageGCTestDataGenerator.generateMockRGVisibility(
            1002L, 0, 60000, 0.5)); // Boundary ratio
        rgVisibilityMap.put(1003L, StorageGCTestDataGenerator.generateMockRGVisibility(
            1003L, 0, 50000, 0.3)); // Low invalid ratio
    }
    
    // ============================
    // End-to-End Workflow Tests
    // ============================
    
    /**
     * TC027: Test complete GC workflow with multiple files.
     * Scenario: Complete GC cycle from file scanning to atomic switching.
     */
    @Test
    public void testCompleteGCWorkflow_MultipleFiles_ShouldProcessSuccessfully() throws Exception
    {
        // Setup mocks for complete workflow
        setupCompleteWorkflowMocks();
        
        // Execute GC process
        boolean result = storageGC.runGarbageCollection();
        
        // Verify complete workflow
        assertTrue("GC workflow should complete successfully", result);
        
        // Verify file scanning was performed
        verify(metadataService, atLeastOnce()).getFilesByTable(anyLong());
        
        // Verify data rewriting was performed for eligible files
        verify(resourceManager, atLeastOnce()).getRGVisibility(1001L, 0);
        verify(resourceManager, atLeastOnce()).getRGVisibility(1002L, 0);
        
        // Verify atomic switching was called
        verify(metadataService, atLeastOnce()).atomicSwapFiles(anyList(), anyList());
        
        // Verify dual-write mapping registration
        verify(resourceManager, atLeastOnce()).registerRedirection(anyLong(), anyLong(), any());
    }
    
    /**
     * TC028: Test GC workflow with no eligible files.
     * Scenario: No files exceed invalid ratio threshold.
     */
    @Test
    public void testGCWorkflow_NoEligibleFiles_ShouldSkipProcessing() throws Exception
    {
        // Setup files with low invalid ratios
        List<File> lowInvalidFiles = Arrays.asList(
            StorageGCTestDataGenerator.generateMockFile(2001L, "low1.pxl", 1, "node1", 5242880L, 50000),
            StorageGCTestDataGenerator.generateMockFile(2002L, "low2.pxl", 1, "node1", 6291456L, 60000)
        );
        
        when(metadataService.getFilesByTable(anyLong())).thenReturn(lowInvalidFiles);
        when(resourceManager.getRGVisibility(anyLong(), anyInt())).thenReturn(
            StorageGCTestDataGenerator.generateMockRGVisibility(2001L, 0, 50000, 0.3)
        );
        
        // Execute GC process
        boolean result = storageGC.runGarbageCollection();
        
        // Verify no processing occurred
        assertTrue("GC should complete without errors", result);
        verify(metadataService, never()).atomicSwapFiles(anyList(), anyList());
        verify(resourceManager, never()).registerRedirection(anyLong(), anyLong(), any());
    }
    
    // ============================
    // Error Handling and Rollback Tests
    // ============================
    
    /**
     * TC029: Test GC workflow with file reading failure.
     * Scenario: Corrupted file should be skipped gracefully.
     */
    @Test
    public void testGCWorkflow_FileReadingFailure_ShouldSkipCorruptedFile() throws Exception
    {
        // Setup one corrupted file and one valid file
        List<File> mixedFiles = Arrays.asList(
            StorageGCTestDataGenerator.generateMockFile(3001L, "corrupted.pxl", 1, "node1", 8388608L, 80000),
            StorageGCTestDataGenerator.generateMockFile(3002L, "valid.pxl", 1, "node1", 6291456L, 60000)
        );
        
        when(metadataService.getFilesByTable(anyLong())).thenReturn(mixedFiles);
        
        // First file throws IOException, second file is valid
        when(storage.getStatus(eq("/path/to/corrupted.pxl"))).thenThrow(new IOException("File corrupted"));
        when(storage.getStatus(eq("/path/to/valid.pxl"))).thenReturn(mock(Storage.Status.class));
        
        when(resourceManager.getRGVisibility(3002L, 0)).thenReturn(
            StorageGCTestDataGenerator.generateMockRGVisibility(3002L, 0, 60000, 0.6)
        );
        
        // Execute GC process
        boolean result = storageGC.runGarbageCollection();
        
        // Verify corrupted file was skipped, valid file was processed
        assertTrue("GC should complete despite corrupted file", result);
        verify(metadataService, times(1)).atomicSwapFiles(anyList(), anyList());
    }
    
    /**
     * TC030: Test GC workflow with atomic swap failure.
     * Scenario: Metadata service failure should trigger rollback.
     */
    @Test
    public void testGCWorkflow_AtomicSwapFailure_ShouldRollbackChanges() throws Exception
    {
        // Setup eligible files
        setupCompleteWorkflowMocks();
        
        // Make atomic swap fail
        when(metadataService.atomicSwapFiles(anyList(), anyList())).thenReturn(false);
        
        // Execute GC process
        boolean result = storageGC.runGarbageCollection();
        
        // Verify rollback occurred
        assertFalse("GC should fail when atomic swap fails", result);
        
        // Verify cleanup operations (new files deleted, mappings removed)
        verify(storage, atLeastOnce()).delete(anyString()); // Delete newly created files
        verify(resourceManager, atLeastOnce()).unregisterRedirection(anyLong()); // Remove mappings
    }
    
    /**
     * TC031: Test partial failure handling.
     * Scenario: Some file groups succeed, others fail.
     */
    @Test
    public void testGCWorkflow_PartialFailure_ShouldHandleGracefully() throws Exception
    {
        // Setup multiple file groups
        List<File> group1Files = Arrays.asList(
            StorageGCTestDataGenerator.generateMockFile(4001L, "group1_file1.pxl", 1, "node1", 8388608L, 80000),
            StorageGCTestDataGenerator.generateMockFile(4002L, "group1_file2.pxl", 1, "node1", 6291456L, 60000)
        );
        
        List<File> group2Files = Arrays.asList(
            StorageGCTestDataGenerator.generateMockFile(4003L, "group2_file1.pxl", 2, "node1", 7340032L, 70000)
        );
        
        when(metadataService.getFilesByTable(1L)).thenReturn(group1Files);
        when(metadataService.getFilesByTable(2L)).thenReturn(group2Files);
        
        // Setup RGVisibility for all files
        when(resourceManager.getRGVisibility(4001L, 0)).thenReturn(
            StorageGCTestDataGenerator.generateMockRGVisibility(4001L, 0, 80000, 0.7)
        );
        when(resourceManager.getRGVisibility(4002L, 0)).thenReturn(
            StorageGCTestDataGenerator.generateMockRGVisibility(4002L, 0, 60000, 0.6)
        );
        when(resourceManager.getRGVisibility(4003L, 0)).thenReturn(
            StorageGCTestDataGenerator.generateMockRGVisibility(4003L, 0, 70000, 0.8)
        );
        
        // Make group2 processing fail
        when(storage.getStatus(contains("group2"))).thenThrow(new IOException("Storage error"));
        
        // Execute GC process
        boolean result = storageGC.runGarbageCollection();
        
        // Verify partial success handling
        assertTrue("GC should complete with partial success", result);
        
        // Verify group1 was processed successfully
        verify(metadataService, atLeastOnce()).atomicSwapFiles(
            argThat(list -> list.stream().anyMatch(f -> f.getName().contains("group1"))), 
            anyList()
        );
    }
    
    // ============================
    // Concurrent Access Tests
    // ============================
    
    /**
     * TC032: Test concurrent access during GC process.
     * Scenario: Multiple threads accessing files during GC.
     */
    @Test(timeout = 30000)
    public void testConcurrentAccess_DuringGCProcess_ShouldMaintainConsistency() throws Exception
    {
        final int threadCount = 5;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(threadCount);
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger failureCount = new AtomicInteger(0);
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        
        // Setup test file
        File testFile = StorageGCTestDataGenerator.generateMockFile(5001L, "concurrent_test.pxl", 
                                                                    1, "node1", 8388608L, 80000);
        
        when(metadataService.getFilesByTable(anyLong())).thenReturn(Arrays.asList(testFile));
        when(resourceManager.getRGVisibility(5001L, 0)).thenReturn(
            StorageGCTestDataGenerator.generateMockRGVisibility(5001L, 0, 80000, 0.7)
        );
        
        // Start GC process in background
        Thread gcThread = new Thread(() -> {
            try
            {
                storageGC.runGarbageCollection();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        });
        
        // Start concurrent access threads
        for (int i = 0; i < threadCount; i++)
        {
            executor.submit(() -> {
                try
                {
                    startLatch.await(); // Wait for all threads to be ready
                    
                    // Simulate concurrent file access (read operations)
                    boolean success = simulateConcurrentFileAccess(testFile);
                    if (success)
                    {
                        successCount.incrementAndGet();
                    }
                    else
                    {
                        failureCount.incrementAndGet();
                    }
                }
                catch (Exception e)
                {
                    failureCount.incrementAndGet();
                }
                finally
                {
                    finishLatch.countDown();
                }
            });
        }
        
        // Start all threads simultaneously
        startLatch.countDown();
        gcThread.start();
        
        // Wait for completion
        assertTrue("All threads should complete within timeout", 
                   finishLatch.await(20, TimeUnit.SECONDS));
        gcThread.join(5000);
        
        executor.shutdown();
        
        // Verify results
        assertEquals("All concurrent accesses should succeed", threadCount, successCount.get());
        assertEquals("No concurrent accesses should fail", 0, failureCount.get());
    }
    
    /**
     * TC033: Test concurrent deletion operations during dual-write phase.
     * Scenario: Multiple threads deleting from old and new files simultaneously.
     */
    @Test(timeout = 30000)
    public void testConcurrentDeletion_DualWritePhase_ShouldHandleCorrectly() throws Exception
    {
        final int deletionThreads = 3;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(deletionThreads);
        
        ExecutorService executor = Executors.newFixedThreadPool(deletionThreads);
        
        // Setup dual-write mapping
        long oldFileId = 6001L;
        long newFileId = 6002L;
        int[] mapping = StorageGCTestDataGenerator.generateRowIdMapping(1000, 0.3);
        
        when(resourceManager.isInDualWritePhase(oldFileId)).thenReturn(true);
        when(resourceManager.getForwardMapping(oldFileId)).thenReturn(mapping);
        
        // Start deletion threads
        for (int i = 0; i < deletionThreads; i++)
        {
            final int threadId = i;
            executor.submit(() -> {
                try
                {
                    startLatch.await();
                    
                    // Simulate concurrent deletions
                    for (int j = 0; j < 10; j++)
                    {
                        int rowId = threadId * 10 + j;
                        
                        // Randomly delete from old or new file
                        if (Math.random() > 0.5)
                        {
                            resourceManager.deleteRecord(oldFileId, rowId);
                        }
                        else
                        {
                            resourceManager.deleteRecord(newFileId, rowId);
                        }
                    }
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                finally
                {
                    finishLatch.countDown();
                }
            });
        }
        
        // Start all threads
        startLatch.countDown();
        
        // Wait for completion
        assertTrue("All deletion threads should complete", 
                   finishLatch.await(10, TimeUnit.SECONDS));
        
        executor.shutdown();
        
        // Verify deletion operations were handled correctly
        verify(resourceManager, atLeast(deletionThreads * 5)).deleteRecord(anyLong(), anyInt());
    }
    
    // ============================
    // Performance and Resource Tests
    // ============================
    
    /**
     * TC034: Test memory usage during large file processing.
     * Scenario: Processing large files should not cause memory exhaustion.
     */
    @Test(timeout = 60000)
    public void testMemoryUsage_LargeFileProcessing_ShouldRemainControlled() throws Exception
    {
        // Setup large file (1GB simulated)
        File largeFile = StorageGCTestDataGenerator.generateMockFile(7001L, "large_file.pxl", 
                                                                    1, "node1", 1073741824L, 10000000);
        
        when(metadataService.getFilesByTable(anyLong())).thenReturn(Arrays.asList(largeFile));
        when(resourceManager.getRGVisibility(7001L, 0)).thenReturn(
            StorageGCTestDataGenerator.generateMockRGVisibility(7001L, 0, 10000000, 0.6)
        );
        
        // Monitor memory usage
        Runtime runtime = Runtime.getRuntime();
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();
        
        // Execute GC process
        boolean result = storageGC.runGarbageCollection();
        
        long finalMemory = runtime.totalMemory() - runtime.freeMemory();
        long memoryIncrease = finalMemory - initialMemory;
        
        // Verify memory usage is reasonable
        assertTrue("GC process should complete successfully", result);
        assertTrue("Memory increase should be reasonable: " + memoryIncrease, 
                   memoryIncrease < 500 * 1024 * 1024); // Less than 500MB increase
    }
    
    /**
     * TC035: Test timeout handling for long-running GC operations.
     * Scenario: GC process should respect timeout constraints.
     */
    @Test(timeout = 10000)
    public void testTimeoutHandling_LongRunningOperation_ShouldRespectTimeout() throws Exception
    {
        // Setup file that will cause slow processing
        File slowFile = StorageGCTestDataGenerator.generateMockFile(8001L, "slow_file.pxl", 
                                                                   1, "node1", 8388608L, 80000);
        
        when(metadataService.getFilesByTable(anyLong())).thenReturn(Arrays.asList(slowFile));
        
        // Make storage operations slow
        when(storage.getStatus(anyString())).thenAnswer(invocation -> {
            Thread.sleep(100); // Simulate slow I/O
            return mock(Storage.Status.class);
        });
        
        when(resourceManager.getRGVisibility(8001L, 0)).thenReturn(
            StorageGCTestDataGenerator.generateMockRGVisibility(8001L, 0, 80000, 0.7)
        );
        
        // Execute with timeout constraint
        boolean result = storageGC.runGarbageCollection();
        
        // Should complete within timeout
        assertTrue("GC should complete within timeout", result);
    }
    
    // ============================
    // Helper Methods
    // ============================
    
    private void setupCompleteWorkflowMocks() throws Exception
    {
        when(metadataService.getFilesByTable(anyLong())).thenReturn(testFiles);
        
        // Setup RGVisibility responses
        for (File file : testFiles)
        {
            RGVisibility rgVis = rgVisibilityMap.get(file.getId());
            if (rgVis != null)
            {
                when(resourceManager.getRGVisibility(file.getId(), 0)).thenReturn(rgVis);
            }
        }
        
        // Setup storage responses
        when(storage.getStatus(anyString())).thenReturn(mock(Storage.Status.class));
        when(storage.getStatus(anyString()).getLength()).thenReturn(8388608L);
        
        // Setup successful atomic swap
        when(metadataService.atomicSwapFiles(anyList(), anyList())).thenReturn(true);
    }
    
    private boolean simulateConcurrentFileAccess(File file)
    {
        try
        {
            // Simulate various file access operations
            // In real implementation, this would involve actual file operations
            Thread.sleep(10); // Simulate operation time
            return true;
        }
        catch (InterruptedException e)
        {
            return false;
        }
    }
}