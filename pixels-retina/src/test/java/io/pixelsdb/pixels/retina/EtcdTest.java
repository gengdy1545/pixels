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

import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;

import org.junit.Test;
import org.junit.Assert;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class EtcdTest
{
    @Test
    public void testBasic()
    {
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        etcdUtil.putKeyValue("key1", "value1");
        KeyValue keyValue = etcdUtil.getKeyValue("key1");
        System.out.println(keyValue.getValue().toString(StandardCharsets.UTF_8));
        etcdUtil.delete("key1");
    }

    @Test
    public void testEtcdTransaction() throws Exception
    {
        EtcdUtil etcdUtil = EtcdUtil.Instance();

        // prepare data
        String testPrefix = "test/transaction/";
        String flagKey = testPrefix + "writing_flag";
        String bufferKey = testPrefix + "buffer_0";
        String testValue = "test_value";
        byte[] testBytes = testValue.getBytes(StandardCharsets.UTF_8);

        try
        {
            // clean up previous data
            etcdUtil.deleteByPrefix(testPrefix);
            etcdUtil.delete(flagKey);

            // transaction 1
            etcdUtil.putKeyValue(flagKey, "false");
            Txn txn1a = etcdUtil.createTransaction();
            txn1a = etcdUtil.ifThen(txn1a, flagKey, "false", Cmp.Op.EQUAL);
            txn1a = etcdUtil.thenPut(txn1a, flagKey, "true");
            TxnResponse response1a = etcdUtil.commitTransaction(txn1a).get();
            System.out.println("Transaction 1a: " + response1a.isSucceeded());
            Txn txn1b = etcdUtil.createTransaction();
            txn1b = etcdUtil.thenPut(txn1b, bufferKey, testBytes);
            txn1b = etcdUtil.thenPut(txn1b, flagKey, "false");
            TxnResponse response1b = etcdUtil.commitTransaction(txn1b).get();
            KeyValue bufferValue = etcdUtil.getKeyValue(bufferKey);
            System.out.println("Transaction 1b: " + response1b.isSucceeded() + ", buffer value: " + bufferValue.getValue().toString(StandardCharsets.UTF_8));
            KeyValue flagValue = etcdUtil.getKeyValue(flagKey);
            System.out.println("Transaction 1b: flag value: " + flagValue.getValue().toString(StandardCharsets.UTF_8));

            // transaction 2
            etcdUtil.putKeyValue(flagKey, "true");
            Txn txn2 = etcdUtil.createTransaction();
            txn2 = etcdUtil.ifThen(txn2, flagKey, "false", Cmp.Op.EQUAL);
            txn2 = etcdUtil.thenPut(txn2, bufferKey, "should_not_be_set");
            txn2 = etcdUtil.elsePut(txn2, flagKey, "false");
            TxnResponse response2 = etcdUtil.commitTransaction(txn2).get();
            bufferValue = etcdUtil.getKeyValue(bufferKey);
            System.out.println("Transaction 2: " + response2.isSucceeded() + ", buffer value: " + bufferValue.getValue().toString(StandardCharsets.UTF_8));
            flagValue = etcdUtil.getKeyValue(flagKey);
            System.out.println("Transaction 2: flag value: " + flagValue.getValue().toString(StandardCharsets.UTF_8));

            // transaction 3
            Txn txn3 = etcdUtil.createTransaction();
            txn3 = etcdUtil.thenDelete(txn3, bufferKey);
            TxnResponse response3 = etcdUtil.commitTransaction(txn3).get();
            bufferValue = etcdUtil.getKeyValue(bufferKey);
            System.out.println("Transaction 3: " + response3.isSucceeded() + ", buffer value: " + bufferValue);

            // trnasaction 4
            for (int i = 1; i < 10; ++i)
            {
                etcdUtil.putKeyValue(testPrefix + "buffer_" + i, "buffer_content_" + i);
            }
            etcdUtil.getKeyValuesByPrefix(testPrefix + "buffer_").forEach(kv ->
                System.out.println(kv.getKey().toString(StandardCharsets.UTF_8) + ": " +
                                  kv.getValue().toString(StandardCharsets.UTF_8)));
            Txn txn4 = etcdUtil.createTransaction();
            for (int i = 1; i < 10; ++i)
            {
                txn4 = etcdUtil.thenDelete(txn4, testPrefix + "buffer_" + i);
            }
            TxnResponse response4 = etcdUtil.commitTransaction(txn4).get();
            System.out.println("Transaction 4: " + response4.isSucceeded());
            etcdUtil.getKeyValuesByPrefix(testPrefix + "buffer_").forEach(kv ->
                System.out.println(kv.getKey().toString(StandardCharsets.UTF_8) + ": " +
                                  kv.getValue().toString(StandardCharsets.UTF_8)));
        } finally
        {
            etcdUtil.deleteByPrefix(testPrefix);  // clean up
        }
    }

    @Test
    public void testConcurrentBufferAccess() throws Exception
    {
        EtcdUtil etcdUtil = EtcdUtil.Instance();

        String testPrefix = "test/concurrent/";
        String flagKey = testPrefix + "writing_flag";
        String bufferKey = testPrefix + "buffer_data";
        String finalBufferContent = "final_buffer_data";
        String tempBufferContent = "temp_buffer_data";
        byte[] finalBufferBytes = finalBufferContent.getBytes(StandardCharsets.UTF_8);

        final int numReaders = 20; // Simulate multiple concurrent readers
        final int numIterations = 50; // Number of iterations per thread
        
        try {
            // clean up previous data and verify it's gone
            etcdUtil.deleteByPrefix(testPrefix);
            assert(etcdUtil.getKeyValue(flagKey) == null);

            // Initialize flag and verify it exists
            etcdUtil.putKeyValue(flagKey, "false");
            assert(Objects.equals(etcdUtil.getKeyValue(flagKey).getValue().toString(StandardCharsets.UTF_8), "false"));

            ExecutorService executor = Executors.newFixedThreadPool(numReaders + 1);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch endLatch = new CountDownLatch(numReaders + 1);
            
            AtomicBoolean foundInconsistentState = new AtomicBoolean(false);
            AtomicInteger accessDuringWritingCount = new AtomicInteger(0);

            // start a writing thread
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    
                    for (int i = 0; i < numIterations; i++) {
                        try {
                            // 0. Clean up buffer data but keep the flag
                            etcdUtil.delete(bufferKey);
                            assert(etcdUtil.getKeyValue(bufferKey) == null);

                            // 1. Set flag to true via transaction
                            Txn txn1 = etcdUtil.createTransaction();
                            txn1 = etcdUtil.ifThen(txn1, flagKey, "false", Cmp.Op.EQUAL);
                            txn1 = etcdUtil.thenPut(txn1, flagKey, "true");
                            TxnResponse response1 = etcdUtil.commitTransaction(txn1).get(); // Wait for completion

                            if (response1.isSucceeded()) {
                                // Verify flag was set to true
                                KeyValue afterTxn1 = etcdUtil.getKeyValue(flagKey);
                                if (afterTxn1 == null || !"true".equals(afterTxn1.getValue().toString(StandardCharsets.UTF_8))) {
                                    System.out.println("Writer: Flag was not set to true after txn1");
                                }
                                
                                // Simulate temporary state during write
                                etcdUtil.putKeyValue(bufferKey, tempBufferContent);
                                Thread.sleep(10); // simulate some delay during write
                                
                                // 2. Complete write by setting final data and resetting flag to false
                                Txn txn2 = etcdUtil.createTransaction();
                                txn2 = etcdUtil.thenPut(txn2, bufferKey, finalBufferBytes);
                                txn2 = etcdUtil.thenPut(txn2, flagKey, "false");
                                etcdUtil.commitTransaction(txn2).get(); // Wait for completion
                                
                                // Verify flag was set back to false
                                KeyValue afterTxn2 = etcdUtil.getKeyValue(flagKey);
                                if (afterTxn2 == null || !"false".equals(afterTxn2.getValue().toString(StandardCharsets.UTF_8))) {
                                    System.out.println("Writer: Flag was not set back to false after txn2");
                                }
                            } else {
                                System.out.println("Writer: Transaction to set writing flag failed on iteration " + i);
                            }
                        } catch (Exception e) {
                            System.out.println("Writer: Exception during iteration " + i);
                            e.printStackTrace();
                        }
                        
                        Thread.sleep(5); // Give reader threads some time
                    }
                } catch (Exception e) {
                    System.out.println("Writer: Exception in main loop");
                    e.printStackTrace();
                } finally {
                    endLatch.countDown();
                }
            });
            
            // start multiple reading threads
            for (int r = 0; r < numReaders; r++) {
                final int readerId = r;
                executor.submit(() -> {
                    try {
                        startLatch.await(); // Wait for all threads to be ready
                        
                        for (int i = 0; i < numIterations; i++) {
                            try {
                                KeyValue currentFlagValue = etcdUtil.getKeyValue(flagKey);
                                assert(currentFlagValue != null);
                                String flagState = currentFlagValue.getValue().toString(StandardCharsets.UTF_8);

                                // If flag is false, we should not be able to read the intermedia buffer data
                                if ("false".equals(flagState)) {
                                    KeyValue bufferValue = etcdUtil.getKeyValue(bufferKey);
                                    if (bufferValue != null) {
                                        String bufferContent = bufferValue.getValue().toString(StandardCharsets.UTF_8);

                                        // If we can read the intermedia buffer data while flag is true,
                                        // it's an inconsistent state
                                        if (tempBufferContent.equals(bufferContent)) {
                                            foundInconsistentState.set(true);
                                            System.out.println("Reader " + readerId + ": Inconsistent state detected: flag=true but final data was read");
                                        }

                                        // Count accesses during writing state
                                        accessDuringWritingCount.incrementAndGet();
                                    }
                                }
                            } catch (Exception e) {
                                System.out.println("Reader " + readerId + ": Exception during iteration " + i);
                                e.printStackTrace();
                            }
                            
                            // Add some random delay to increase concurrency
                            Thread.sleep((long) (Math.random() * 5));
                        }
                    } catch (Exception e) {
                        System.out.println("Reader " + readerId + ": Exception in main loop");
                        e.printStackTrace();
                    } finally {
                        endLatch.countDown();
                    }
                });
            }
            startLatch.countDown(); // Start all threads
            
            // Wait for all threads to complete
            endLatch.await();
            executor.shutdown();
            
            // Verify test results
            System.out.println("Number of accesses during writing state: " + accessDuringWritingCount.get());
            System.out.println("Inconsistent state detected: " + foundInconsistentState.get());
            
            // Test fails if inconsistent state was found
            Assert.assertFalse("Should not be able to read intermedia buffer data when writing_flag is false", foundInconsistentState.get());
            
        } finally {
            // Clean up test data
            etcdUtil.deleteByPrefix(testPrefix);
        }
    }
}
