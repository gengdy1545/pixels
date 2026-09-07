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
package io.pixelsdb.pixels.daemon.transaction;

import io.pixelsdb.pixels.common.exception.EtcdException;
import io.pixelsdb.pixels.common.transaction.TransContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestTransServiceImpl
{
    @Test
    public void testBeginRegistrationIsAtomicWithFenceAndBoundary() throws Exception
    {
        BlockingTransContextManager contextManager = new BlockingTransContextManager(false);
        TransServiceImpl.OrderedTransIdSequence sequence = new TransServiceImpl.OrderedTransIdSequence(
                new InMemoryTransIdAllocator(100L), contextManager, "incarnation-single");
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try
        {
            Future<TransContext> begin = executor.submit(() -> sequence.beginTrans(true, 10L));
            assertTrue(contextManager.awaitRegistration());

            CountDownLatch contendersStarted = new CountDownLatch(2);
            Future<Long> fence = executor.submit(() -> {
                contendersStarted.countDown();
                return sequence.allocatePlacementFence();
            });
            Future<TransServiceImpl.PlacementBoundarySnapshot> boundary = executor.submit(() -> {
                contendersStarted.countDown();
                return sequence.getPlacementSafeBoundary();
            });
            assertTrue(contendersStarted.await(5, TimeUnit.SECONDS));
            assertBlocked(fence);
            assertBlocked(boundary);

            contextManager.releaseRegistration();
            TransContext context = begin.get(5, TimeUnit.SECONDS);
            long placementFence = fence.get(5, TimeUnit.SECONDS);
            TransServiceImpl.PlacementBoundarySnapshot snapshot = boundary.get(5, TimeUnit.SECONDS);

            assertEquals(100L, context.getTransId());
            assertEquals(101L, placementFence);
            assertEquals(context.getTransId(), snapshot.getSafeBoundary());
            assertEquals("incarnation-single", snapshot.getServiceIncarnation());
            assertNull(contextManager.getTransContext(placementFence));

            assertTrue(contextManager.markTransOffloaded(context.getTransId()));
            assertEquals(-1L, contextManager.getMinRunningTransTimestamp(true));
            assertEquals(context.getTransId(), sequence.getPlacementSafeBoundary().getSafeBoundary());

            assertTrue(sequence.setTransRollback(context.getTransId()));
            assertEquals(placementFence + 1, sequence.getPlacementSafeBoundary().getSafeBoundary());
        }
        finally
        {
            contextManager.releaseRegistration();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testBatchRegistrationIsAtomicWithFenceAndBoundary() throws Exception
    {
        BlockingTransContextManager contextManager = new BlockingTransContextManager(true);
        TransServiceImpl.OrderedTransIdSequence sequence = new TransServiceImpl.OrderedTransIdSequence(
                new InMemoryTransIdAllocator(200L), contextManager, "incarnation-batch");
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try
        {
            Future<TransContext[]> beginBatch =
                    executor.submit(() -> sequence.beginTransBatch(3, true, 20L));
            assertTrue(contextManager.awaitRegistration());

            CountDownLatch contendersStarted = new CountDownLatch(2);
            Future<Long> fence = executor.submit(() -> {
                contendersStarted.countDown();
                return sequence.allocatePlacementFence();
            });
            Future<TransServiceImpl.PlacementBoundarySnapshot> boundary = executor.submit(() -> {
                contendersStarted.countDown();
                return sequence.getPlacementSafeBoundary();
            });
            assertTrue(contendersStarted.await(5, TimeUnit.SECONDS));
            assertBlocked(fence);
            assertBlocked(boundary);

            contextManager.releaseRegistration();
            TransContext[] contexts = beginBatch.get(5, TimeUnit.SECONDS);
            long placementFence = fence.get(5, TimeUnit.SECONDS);
            TransServiceImpl.PlacementBoundarySnapshot snapshot = boundary.get(5, TimeUnit.SECONDS);

            assertEquals(200L, contexts[0].getTransId());
            assertEquals(201L, contexts[1].getTransId());
            assertEquals(202L, contexts[2].getTransId());
            assertEquals(203L, placementFence);
            assertEquals(contexts[0].getTransId(), snapshot.getSafeBoundary());
            assertEquals("incarnation-batch", snapshot.getServiceIncarnation());

            assertTrue(contextManager.markTransOffloaded(contexts[1].getTransId()));
            List<Boolean> success = new ArrayList<>();
            assertTrue(sequence.setTransCommitBatch(
                    Arrays.asList(contexts[0].getTransId(), contexts[2].getTransId()), success));
            assertEquals(Arrays.asList(true, true), success);
            assertEquals(contexts[1].getTransId(), sequence.getPlacementSafeBoundary().getSafeBoundary());

            assertTrue(sequence.setTransRollback(contexts[1].getTransId()));
            assertEquals(placementFence + 1, sequence.getPlacementSafeBoundary().getSafeBoundary());
        }
        finally
        {
            contextManager.releaseRegistration();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static void assertBlocked(Future<?> future) throws Exception
    {
        try
        {
            future.get(100, TimeUnit.MILLISECONDS);
            fail("operation crossed the transaction id allocation-to-registration window");
        }
        catch (TimeoutException expected)
        {
            assertFalse(future.isDone());
        }
    }

    private static final class InMemoryTransIdAllocator implements TransServiceImpl.TransactionIdAllocator
    {
        private final AtomicLong nextTransId;

        private InMemoryTransIdAllocator(long firstTransId)
        {
            this.nextTransId = new AtomicLong(firstTransId);
        }

        @Override
        public long getAndIncrement() throws EtcdException
        {
            return nextTransId.getAndIncrement();
        }

        @Override
        public long getAndIncrement(int batchSize) throws EtcdException
        {
            return nextTransId.getAndAdd(batchSize);
        }
    }

    private static final class BlockingTransContextManager extends TransContextManager
    {
        private final boolean blockBatch;
        private final CountDownLatch registrationEntered = new CountDownLatch(1);
        private final CountDownLatch allowRegistration = new CountDownLatch(1);

        private BlockingTransContextManager(boolean blockBatch)
        {
            this.blockBatch = blockBatch;
        }

        @Override
        public void addTransContext(TransContext context)
        {
            if (!blockBatch)
            {
                blockRegistration();
            }
            super.addTransContext(context);
        }

        @Override
        public void addTransContextBatch(TransContext[] contexts)
        {
            if (blockBatch)
            {
                blockRegistration();
            }
            super.addTransContextBatch(contexts);
        }

        private void blockRegistration()
        {
            registrationEntered.countDown();
            try
            {
                if (!allowRegistration.await(5, TimeUnit.SECONDS))
                {
                    throw new AssertionError("timed out waiting to release transaction context registration");
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new AssertionError("transaction context registration was interrupted", e);
            }
        }

        private boolean awaitRegistration() throws InterruptedException
        {
            return registrationEntered.await(5, TimeUnit.SECONDS);
        }

        private void releaseRegistration()
        {
            allowRegistration.countDown();
        }
    }
}
