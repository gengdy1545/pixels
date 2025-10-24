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

    public static class RocksDBUtil
    {
        private static final ByteOrder ENDIAN = ByteOrder.LITTLE_ENDIAN;
        private static final int KEY_IDENTITY_LENGTH = 8; // indexId(4) + key(4)
        private static final int FULL_KEY_LENGTH = 16;   // Key Identity (8) + Timestamp (8)
        private static final int VALUE_LENGTH = 8;      // rowId(8)

        public static byte[] encodeKey(Config cfg, int indexId, int key, long timestamp)
        {
            ByteBuffer buffer = ByteBuffer.allocate(FULL_KEY_LENGTH).order(ENDIAN);
            buffer.putInt(indexId).putInt(key);

            if (cfg.tsType == TsType.EMBED_ASC)
            {
                buffer.putLong(timestamp);
            }
            else if (cfg.tsType == TsType.EMBED_DESC)
            {
                buffer.putLong(Long.MAX_VALUE - timestamp);
            }
            else
            {
                throw new IllegalArgumentException("Unknown TsType: " + cfg.tsType);
            }
            return buffer.array();
        }

        public static byte[] encodeValue(long rowId)
        {
            return ByteBuffer.allocate(VALUE_LENGTH).order(ENDIAN).putLong(rowId).array();
        }

        public static long decodeLong(byte[] bytes)
        {
            if (bytes == null || bytes.length < VALUE_LENGTH)
            {
                return -1;
            }
            return ByteBuffer.wrap(bytes).order(ENDIAN).getLong();
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
                cfDescriptors.add(new ColumnFamilyDescriptor(name, new ColumnFamilyOptions()));

                if (config.tsType == TsType.EMBED_DESC)
                {
                    BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
                    tableConfig.setFilterPolicy(new BloomFilter(10, false));
                    tableConfig.setWholeKeyFiltering(false);
                    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
                    cfOptions.setTableFormatConfig(tableConfig);
                    cfOptions.useFixedLengthPrefixExtractor(8);
                    cfDescriptors.add(new ColumnFamilyDescriptor(name, cfOptions));
                }
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
    public void tearDown() throws Exception
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

    private class Worker implements Runnable
    {
        private final int indexId;
        private final AtomicLong counter;
        private final ColumnFamilyHandle cfh;

        public Worker(int indexId, AtomicLong counter)
        {
            this.indexId = indexId;
            this.counter = counter;
            this.cfh = handles.get(0);
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
                    // 使用毫秒时间戳作为当前操作的时间 T_query
                    long tQuery = System.currentTimeMillis();

                    // 1. Read: 查找 Key 的最新版本 (<= T_query) 并获取其 rowId
                    long oldRow = getUniqueRowId(key, tQuery, ropt);

                    // 2. Compute new row ID
                    long newRow = (oldRow == -1) ? ThreadLocalRandom.current().nextLong() : oldRow + 1;

                    // 3. Write: 写入新版本数据
                    // 注意：写入时间戳通常是 T_query，或一个新的递增时间戳
                    long tsWrite = tQuery; // 使用查询时间作为写入时间
                    byte[] k = RocksDBUtil.encodeKey(config, indexId, key, tsWrite);
                    byte[] v = RocksDBUtil.encodeValue(newRow);

                    db.put(cfh, wopt, k, v);
                    counter.incrementAndGet();
                }
            } catch (RocksDBException e)
            {
                System.err.println("RocksDB operation failed in thread " + indexId + ": " + e.getMessage());
            }
        }

        private long getUniqueRowId(int key, long tQuery, ReadOptions ropt) throws RocksDBException
        {
            // Key Identity 前缀 (indexId + key)
            byte[] keyPrefix = RocksDBUtil.encodeKey(config, indexId, key, 0);

            try (RocksIterator it = db.newIterator(cfh, ropt))
            {

                if (config.tsType == TsType.EMBED_ASC)
                {
                    // EMBED_ASC: Key = [ID|Key|Timestamp]
                    // 目标：找到 Key Identity 相同且 Timestamp <= T_query 的最大 Key。
                    // C++ 对应: seekForPrev(Key(ID + Key + T_query))

                    byte[] targetKey = RocksDBUtil.encodeKey(config, indexId, key, tQuery);
                    it.seekForPrev(targetKey);

                } else
                { // EMBED_DESC
                    // EMBED_DESC: Key = [ID|Key|MAX-Timestamp]
                    // 目标：找到 Key Identity 相同且 MAX-Timestamp <= MAX-T_query 的最小 Key。
                    // (即找到 Key Identity 相同且 Timestamp >= T_query 的最小 Key)
                    // C++ 对应: seek(Key(ID + Key + MAX-T_query))

                    byte[] targetKey = RocksDBUtil.encodeKey(config, indexId, key, tQuery);
                    ropt.setPrefixSameAsStart(true);
                    it.seek(targetKey);
                }

                if (it.isValid())
                {
                    // 检查找到的 Key 是否属于目标 Key Identity
                    if (RocksDBUtil.isKeyIdentityMatch(it.key(), keyPrefix))
                    {
                        // 如果匹配，说明找到了 <= T_query (ASC) 或 >= T_query (DESC 反转后) 的最新版本
                        return RocksDBUtil.decodeLong(it.value());
                    }
                }
                return -1;
            }
        }
    }
}
