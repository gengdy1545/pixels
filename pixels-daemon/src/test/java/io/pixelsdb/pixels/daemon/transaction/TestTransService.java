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
package io.pixelsdb.pixels.daemon.transaction;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.daemon.TransProto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestTransService
{
    @Test
    public void testReadTimestampLessThanMinWriteTimestamp() throws Exception
    {
        int writeThreads = 8000;
        int readThreads = 100;
        int testDurationSec = 5;
        AtomicBoolean running = new AtomicBoolean(true);
        ExecutorService writePool = Executors.newFixedThreadPool(writeThreads);
        ExecutorService readPool = Executors.newFixedThreadPool(readThreads);

        List<Long> writeTimestamps = new CopyOnWriteArrayList<>();
        List<Future<?>> writeFutures = new ArrayList<>();
        List<Future<?>> readFutures = new ArrayList<>();

        TransServiceImpl transService = new TransServiceImpl();

        // write transaction threads
        for (int i = 0; i < writeThreads; ++i)
        {
            writeFutures.add(writePool.submit(() -> {
                while (running.get())
                {
                    TransProto.BeginTransRequest req =
                            TransProto.BeginTransRequest.newBuilder()
                                    .setReadOnly(false).build();
                    final long[] transId = new long[1];
                    final long[] ts = new long[1];
                    transService.beginTrans(req, new StreamObserver<TransProto.BeginTransResponse>()
                    {
                        @Override
                        public void onNext(TransProto.BeginTransResponse value)
                        {
                            transId[0] = value.getTransId();
                            ts[0] = value.getTimestamp();
                        }

                        @Override
                        public void onError(Throwable t) {}

                        @Override
                        public void onCompleted() {}
                    });
                    writeTimestamps.add(ts[0]);
                    try
                    {
                        Thread.sleep(2);
                    } catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                    TransProto.CommitTransRequest commitReq =
                            TransProto.CommitTransRequest.newBuilder()
                                    .setTransId(transId[0]).build();
                    transService.commitTrans(commitReq, new StreamObserver<TransProto.CommitTransResponse>()
                    {
                        @Override
                        public void onNext(TransProto.CommitTransResponse value) {}

                        @Override
                        public void onError(Throwable t) {}

                        @Override
                        public void onCompleted() {}
                    });
                    writeTimestamps.remove(ts[0]);
                }
            }));
        }

        // Read-only transaction threads
        for (int i = 0; i < readThreads; ++i)
        {
            readFutures.add(readPool.submit(() -> {
                while (running.get())
                {
                    TransProto.BeginTransRequest req =
                            TransProto.BeginTransRequest.newBuilder()
                                    .setReadOnly(true).build();
                    final long[] readTs = new long[1];
                    transService.beginTrans(req, new StreamObserver<TransProto.BeginTransResponse>()
                    {
                        @Override
                        public void onNext(TransProto.BeginTransResponse value)
                        {
                            readTs[0] = value.getTimestamp();
                        }

                        @Override
                        public void onError(Throwable t) {}

                        @Override
                        public void onCompleted() {}
                    });
                    // check read timestamp < min write timestamp
                    long minWriteTs = Long.MAX_VALUE;
                    for (Long ts: writeTimestamps)
                    {
                        if (ts < minWriteTs)
                        {
                            minWriteTs = ts;
                        }
                    }
                    if (!writeTimestamps.isEmpty())
                    {
                        Assertions.assertTrue(readTs[0] < minWriteTs, "Read ts should be less than min write ts");
                    }
                    try
                    {
                        Thread.sleep(10);
                    } catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            }));
        }

        Thread.sleep(testDurationSec * 1000);
        running.set(false);

        for (Future<?> f : writeFutures)
        {
            f.get();
        }
        for (Future<?> f : readFutures)
        {
            f.get();
        }

        writePool.shutdown();
        readPool.shutdown();
    }
}
