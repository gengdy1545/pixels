package io.pixelsdb.pixels.index.rocksdb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.rocksdb.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestRocksDBPerf
{
    static
    {
        RocksDB.loadLibrary();
    }

    private Config config;
    private RocksDB db;
    private List<ColumnFamilyHandle> handles;

    public enum TsType
    {
        EMBED_ASC("embed_asc"),
        EMBED_DESC("embed_desc");

        private final String name;
        TsType(String name)
        {
            this.name = name;
        }
        @Override
        public String toString()
        {
            return name;
        }
    }

    public enum Preset
    {
        Asc10m,
        Desc10m,
    }

    public static class Config
    {
        public final String dbPath;
        public final int threadNum = 16;
        public final int opsPerThread = 1000000;
        public final boolean destroyBeforeStart = true;

        public final int idRange;
        public final TsType tsType;

        private static final String DB_PATH_PREFIX = "/home/ubuntu/disk1";

        public Config(Preset preset)
        {
            ScaleTypePair p = getScaleAndType(preset);
            this.idRange = p.scale;
            this.tsType = p.type;
            this.dbPath = DB_PATH_PREFIX + File.separator + "test_" + idRange + "_" + tsType;
        }

        private static class ScaleTypePair
        {
            public final int scale;
            public final TsType type;
            public ScaleTypePair(int scale, TsType type)
            {
                this.scale = scale;
                this.type = type;
            }
        }

        private static ScaleTypePair getScaleAndType(Preset preset)
        {
            Objects.requireNonNull(preset);
            final int scale10m = 10000000;
            switch (preset)
            {
                case Asc10m:
                    return new ScaleTypePair(scale10m, TsType.EMBED_ASC);
                case Desc10m:
                    return new ScaleTypePair(scale10m, TsType.EMBED_DESC);
                default:
                    throw new IllegalArgumentException("Unknown preset: " + preset);
            }
        }
    }

    /**
     *
     * 优化后的工具类
     */
    public static class RocksDBUtil
    {
        private static final int KEY_IDENTITY_LENGTH = 8; // indexId(4) + key(4)
        private static final int FULL_KEY_LENGTH = 16;   // Key Identity (8) + Timestamp (8)
        private static final int VALUE_LENGTH = 8;       // rowId(8)

        // --- 新增：用于手动编码的辅助方法 (小端序) ---
        private static void putIntLE(byte[] buffer, int offset, int value) {
            buffer[offset] = (byte) value;
            buffer[offset + 1] = (byte) (value >>> 8);
            buffer[offset + 2] = (byte) (value >>> 16);
            buffer[offset + 3] = (byte) (value >>> 24);
        }

        private static void putLongLE(byte[] buffer, int offset, long value) {
            buffer[offset] = (byte) value;
            buffer[offset + 1] = (byte) (value >>> 8);
            buffer[offset + 2] = (byte) (value >>> 16);
            buffer[offset + 3] = (byte) (value >>> 24);
            buffer[offset + 4] = (byte) (value >>> 32);
            buffer[offset + 5] = (byte) (value >>> 40);
            buffer[offset + 6] = (byte) (value >>> 48);
            buffer[offset + 7] = (byte) (value >>> 56);
        }

        /**
         * 优化后的 Key 编码方法，写入到预分配的缓冲区中
         * @param buffer 目标缓冲区
         */
        public static void encodeKey(Config cfg, int indexId, int key, long timestamp, byte[] buffer)
        {
            putIntLE(buffer, 0, indexId);
            putIntLE(buffer, 4, key);

            long tsValue;
            if (cfg.tsType == TsType.EMBED_ASC)
            {
                tsValue = timestamp;
            }
            else if (cfg.tsType == TsType.EMBED_DESC)
            {
                tsValue = Long.MAX_VALUE - timestamp;
            }
            else
            {
                throw new IllegalArgumentException("Unknown TsType: " + cfg.tsType);
            }
            putLongLE(buffer, 8, tsValue);
        }

        /**
         * 优化后的 Value 编码方法，写入到预分配的缓冲区中
         * @param buffer 目标缓冲区
         */
        public static void encodeValue(long rowId, byte[] buffer)
        {
            putLongLE(buffer, 0, rowId);
        }

        // --- 旧的分配内存的方法，保留给非性能敏感的场景使用 ---
        public static byte[] encodeKey(Config cfg, int indexId, int key, long timestamp)
        {
            byte[] buffer = new byte[FULL_KEY_LENGTH];
            encodeKey(cfg, indexId, key, timestamp, buffer);
            return buffer;
        }

        public static byte[] encodeValue(long rowId)
        {
            byte[] buffer = new byte[VALUE_LENGTH];
            encodeValue(rowId, buffer);
            return buffer;
        }

        // decode 和 isKeyIdentityMatch 几乎没有性能影响，保持原样
        public static long decodeLong(byte[] bytes)
        {
            if (bytes == null || bytes.length < VALUE_LENGTH)
            {
                return -1;
            }
            // ByteBuffer.wrap 不会拷贝数组，开销很小
            return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
        }

        public static boolean isKeyIdentityMatch(byte[] key1, byte[] key2)
        {
            if (key1.length < KEY_IDENTITY_LENGTH || key2.length < KEY_IDENTITY_LENGTH)
            {
                return false;
            }
            for (int i = 0; i < KEY_IDENTITY_LENGTH; i++)
            {
                if (key1[i] != key2[i])
                {
                    return false;
                }
            }
            return true;
        }
    }

    private void fillSequentialData() throws Exception
    {
        final long timestamp = 0;
        final ColumnFamilyHandle cfh = handles.get(0);
        System.out.println("Start sequential data load: " + config.idRange + " entries per thread * " + config.threadNum + " threads");
        List<Thread> threads = new ArrayList<>();
        AtomicLong counter = new AtomicLong(0);

        for (int t = 0; t < config.threadNum; ++t) {
            final int threadId = t;
            threads.add(new Thread(() -> {
                // 在加载数据的线程中也应用缓冲区重用优化
                byte[] k = new byte[RocksDBUtil.FULL_KEY_LENGTH];
                byte[] v = new byte[RocksDBUtil.VALUE_LENGTH];
                try (WriteOptions wopt = new WriteOptions()) {
                    for (int key = 0; key < config.idRange; ++key) {
                        RocksDBUtil.encodeKey(config, threadId, key, timestamp, k);
                        RocksDBUtil.encodeValue(key, v);
                        db.put(cfh, wopt, k, v);
                        counter.incrementAndGet();
                    }
                } catch (RocksDBException e) {
                    System.err.println("Load thread " + threadId + " failed: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }));
        }

        long start = System.currentTimeMillis();
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();
        long end = System.currentTimeMillis();
        System.out.printf("Total loaded: %d entries, time: %.2fs%n",
                counter.get(), (end - start) / 1000.0);
    }

    private RocksDB setupDb(Config config) throws RocksDBException
    {
        this.config = config;
        this.handles = new ArrayList<>();

        if (config.destroyBeforeStart)
        {
            try(Options options = new Options())
            {
                System.out.println("Destroying existing RocksDB at " + config.dbPath);
                RocksDB.destroyDB(config.dbPath, options);
            } catch (RocksDBException e)
            {
            }
        }

        DBOptions dbOptions = null;
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

        try
        {
            List<byte[]> existingCfNames;
            try (Options tempOptions = new Options())
            {
                existingCfNames = RocksDB.listColumnFamilies(tempOptions, config.dbPath);
            } catch (RocksDBException e)
            {
                existingCfNames = new ArrayList<>();
            }

            if (existingCfNames == null || existingCfNames.isEmpty())
            {
                existingCfNames = new ArrayList<>();
                existingCfNames.add(RocksDB.DEFAULT_COLUMN_FAMILY);
            }

            for (byte[] name : existingCfNames)
            {
                ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
                cfOptions.setWriteBufferSize(6 * 1024 * 1024 * 1024); // 6 GB
                if (config.tsType == TsType.EMBED_DESC)
                {
                    BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
                    tableConfig.setFilterPolicy(new BloomFilter(10, false));
                    tableConfig.setWholeKeyFiltering(false);
                    cfOptions.setTableFormatConfig(tableConfig);
                    cfOptions.useFixedLengthPrefixExtractor(8);
                }
                cfDescriptors.add(new ColumnFamilyDescriptor(name, cfOptions));
            }

            dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
            List<ColumnFamilyHandle> rawHandles = new ArrayList<>();
            RocksDB dbInstance = RocksDB.open(dbOptions, config.dbPath, cfDescriptors, rawHandles);
            this.handles.addAll(rawHandles);
            System.out.printf("Opened Rocksdb: %s, CF=%d, ts_type=%s%n",
                    config.dbPath, handles.size(), config.tsType);
            return dbInstance;
        } catch (RocksDBException e)
        {
            if (dbOptions != null)
            {
                dbOptions.close();
            }
            for (ColumnFamilyDescriptor d : cfDescriptors)
            {
                d.getOptions().close();
            }
            throw new RuntimeException("Falied to open RocksDB at " + config.dbPath, e);
        }
    }

    @AfterEach
    public void tearDown()
    {
        if (db != null)
        {
            db.close();
            db = null;
        }
        if (handles != null)
        {
            for (ColumnFamilyHandle handle : handles)
            {
                handle.close();
            }
            handles.clear();
            handles = null;
        }
    }

    @ParameterizedTest(name = "MultiThreadPerf_{0}")
    @EnumSource(Preset.class)
    void testMultiThreadPerf(Preset preset) throws Exception {
        System.out.println("\n--- Starting MultiThreadPerf " + preset + " ---");
        db = setupDb(new Config(preset));
        fillSequentialData();
        System.out.println("Data loading complete. Starting update test.");
        System.err.println("Start update test");
        AtomicLong totalOps = new AtomicLong(0);
        long start = System.nanoTime();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < config.threadNum; ++i) {
            threads.add(new Thread(new Worker(i, totalOps)));
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        long end = System.nanoTime();
        double sec = (end - start) / 1_000_000_000.0;
        long totalOpsCount = totalOps.get();
        System.out.printf("Total ops: %d, time: %.2fs, throughput: %d ops/s%n",
                totalOpsCount, sec, (long) (totalOpsCount / sec));
        assertTrue(totalOpsCount > 0, "Total operations must be greater than zero.");
    }

    /**
     *
     * 优化后的 Worker 类
     */
    private class Worker implements Runnable
    {
        private final int indexId;
        private final AtomicLong counter;
        private final ColumnFamilyHandle cfh;

        // --- 新增：为每个 Worker 线程预分配可重用的缓冲区 ---
        private final byte[] putKeyBuffer;
        private final byte[] putValueBuffer;
        private final byte[] seekKeyBuffer;

        public Worker(int indexId, AtomicLong counter)
        {
            this.indexId = indexId;
            this.counter = counter;
            this.cfh = handles.get(0);

            // 在构造时一次性分配好内存
            this.putKeyBuffer = new byte[RocksDBUtil.FULL_KEY_LENGTH];
            this.putValueBuffer = new byte[RocksDBUtil.VALUE_LENGTH];
            this.seekKeyBuffer = new byte[RocksDBUtil.FULL_KEY_LENGTH];
        }

        @Override
        public void run()
        {
            try (ReadOptions ropt = new ReadOptions();
                 WriteOptions wopt = new WriteOptions())
            {
                for (int i = 0; i < config.opsPerThread; ++i)
                {
                    int key = i % config.idRange;
                    long tQuery = System.currentTimeMillis();

                    // 1. Read: 查找最新版本
                    long oldRow = getUniqueRowId(key, tQuery, ropt);

                    // 2. Compute new row ID
                    long newRow = (oldRow == -1) ? ThreadLocalRandom.current().nextLong() : oldRow + 1;

                    // 3. Write: 写入新版本数据
                    long tsWrite = tQuery;

                    // --- 优化：使用预分配的缓冲区 ---
                    RocksDBUtil.encodeKey(config, indexId, key, tsWrite, this.putKeyBuffer);
                    RocksDBUtil.encodeValue(newRow, this.putValueBuffer);
                    db.put(cfh, wopt, this.putKeyBuffer, this.putValueBuffer);

                    counter.incrementAndGet();
                }
            } catch (RocksDBException e)
            {
                System.err.println("RocksDB operation failed in thread " + indexId + ": " + e.getMessage());
            }
        }

        private long getUniqueRowId(int key, long tQuery, ReadOptions ropt) throws RocksDBException
        {
            try (RocksIterator it = db.newIterator(cfh, ropt))
            {
                // --- 优化：使用预分配的 seekKeyBuffer ---
                RocksDBUtil.encodeKey(config, indexId, key, tQuery, this.seekKeyBuffer);

                if (config.tsType == TsType.EMBED_ASC)
                {
                    it.seekForPrev(this.seekKeyBuffer);
                } else
                { // EMBED_DESC
                    ropt.setPrefixSameAsStart(true);
                    it.seek(this.seekKeyBuffer);
                }

                if (it.isValid())
                {
                    // seekKeyBuffer 的前8个字节就是 key prefix, 可以直接用于比较
                    if (RocksDBUtil.isKeyIdentityMatch(it.key(), this.seekKeyBuffer))
                    {
                        return RocksDBUtil.decodeLong(it.value());
                    }
                }
                return -1;
            }
        }
    }
}