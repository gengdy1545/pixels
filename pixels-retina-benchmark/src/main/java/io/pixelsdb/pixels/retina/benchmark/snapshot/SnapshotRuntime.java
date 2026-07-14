/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
package io.pixelsdb.pixels.retina.benchmark.snapshot;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Prepares disposable physical state and module-scoped semantic configuration before Pixels singletons start. */
public final class SnapshotRuntime implements AutoCloseable
{
    private static final Set<String> INDEX_CONFIG = new HashSet<>(Arrays.asList(
            "index.bucket.num", "index.cache.enabled", "index.cache.capacity",
            "index.cache.expiration.seconds", "index.main.cache.bucket.num",
            "index.rocksdb.multicf", "index.rocksdb.write.buffer.size",
            "index.rocksdb.max.write.buffer.number", "index.rocksdb.max.background.flushes",
            "index.rocksdb.max.background.compactions", "index.rocksdb.max.open.files",
            "index.rocksdb.block.cache.capacity", "index.rocksdb.block.cache.shard.bits",
            "index.rocksdb.block.size", "index.rocksdb.min.write.buffer.number.to.merge",
            "index.rocksdb.level0.file.num.compaction.trigger",
            "index.rocksdb.max.bytes.for.level.base",
            "index.rocksdb.max.bytes.for.level.multiplier",
            "index.rocksdb.target.file.size.base",
            "index.rocksdb.target.file.size.multiplier", "index.rocksdb.prefix.length",
            "index.rocksdb.max.subcompactions", "index.rocksdb.compression.type",
            "index.rocksdb.bottommost.compression.type", "index.rocksdb.compaction.style",
            "index.rocksdb.stats.enabled"));
    private static final Set<String> VISIBILITY_CONFIG = new HashSet<>(Arrays.asList(
            "enabled.storage.schemes", "node.virtual.num",
            "retina.tile.visibility.capacity", "retina.checkpoint.threads"));
    private static final Set<String> WRITE_BUFFER_CONFIG = new HashSet<>(Arrays.asList(
            "pixel.stride", "row.group.size", "block.size", "block.replication",
            "column.chunk.little.endian", "column.chunk.alignment", "isnull.bitmap.alignment",
            "enabled.storage.schemes",
            "node.virtual.num", "index.main.cache.bucket.num",
            "retina.buffer.memTable.size", "retina.buffer.flush.count",
            "retina.buffer.object.flush.threads", "retina.buffer.flush.interval",
            "retina.buffer.flush.encodingLevel", "retina.buffer.flush.nullsPadding",
            "retina.buffer.object.storage.scheme",
            "retina.tile.visibility.capacity"));
    private static final String[] WRITE_BUFFER_REQUIRED_CONFIG = {
            "retina.buffer.memTable.size", "retina.buffer.flush.count",
            "retina.buffer.object.flush.threads", "retina.buffer.flush.interval",
            "retina.buffer.flush.encodingLevel", "retina.buffer.flush.nullsPadding",
            "retina.buffer.object.storage.scheme", "block.size", "block.replication",
            "node.virtual.num", "index.main.cache.bucket.num",
            "retina.tile.visibility.capacity", "enabled.storage.schemes"
    };

    private final Path snapshotDirectory;
    private final SnapshotManifest manifest;
    private final Path workDirectory;
    private final boolean ownsWorkDirectory;

    private SnapshotRuntime(Path snapshotDirectory, SnapshotManifest manifest,
                            Path workDirectory, boolean ownsWorkDirectory)
    {
        this.snapshotDirectory = snapshotDirectory;
        this.manifest = manifest;
        this.workDirectory = workDirectory;
        this.ownsWorkDirectory = ownsWorkDirectory;
    }

    public static SnapshotRuntime open(BenchmarkConfig config) throws IOException
    {
        Path snapshot = Paths.get(config.require("snapshot-dir")).toAbsolutePath().normalize();
        SnapshotManifest manifest = SnapshotIO.readManifest(snapshot);
        String configuredWork = config.get("snapshot-work-dir", null);
        Path work;
        boolean owns;
        if (configuredWork == null || configuredWork.trim().isEmpty())
        {
            work = Files.createTempDirectory("pixels-retina-snapshot-work-");
            owns = true;
        }
        else
        {
            work = Paths.get(configuredWork).toAbsolutePath().normalize();
            if (Files.exists(work) && !isEmptyDirectory(work))
            {
                throw new IOException("--snapshot-work-dir must be empty: " + work);
            }
            Files.createDirectories(work);
            owns = false;
        }
        if (work.startsWith(snapshot) || snapshot.startsWith(work))
        {
            throw new IOException("snapshot and work directories must not contain each other: "
                    + snapshot + " / " + work);
        }
        return new SnapshotRuntime(snapshot, manifest, work, owns);
    }

    public SnapshotManifest manifest()
    {
        return manifest;
    }

    public Path snapshotDirectory()
    {
        return snapshotDirectory;
    }

    public Path workDirectory()
    {
        return workDirectory;
    }

    public SnapshotManifest.TableState requireTable(String table)
    {
        return SnapshotIO.requireTable(manifest, table);
    }

    /** Restore RocksDB + SQLite into a disposable working copy. */
    public void prepareIndexState(SnapshotManifest.TableState table) throws IOException
    {
        if (table.primaryIndex == null)
        {
            throw new IOException("snapshot table has no primary index: " + table.tableName);
        }
        if (!manifest.sourceQuiesced || !manifest.physicalIndexStateIncluded)
        {
            throw new IOException("snapshot does not contain operator-quiesced physical Index state");
        }
        String scheme = table.primaryIndex.scheme.toLowerCase();
        if ("rockset".equals(scheme))
        {
            throw new IOException("Rockset physical restore is not safe or self-contained; use a RocksDB snapshot");
        }
        if (!"rocksdb".equals(scheme))
        {
            throw new IOException("physical snapshot index benchmark currently requires RocksDB, found " + scheme);
        }
        Path sourceRocks = snapshotDirectory.resolve("state/index/rocksdb");
        Path sourceSqlite = snapshotDirectory.resolve("state/index/sqlite");
        Path targetRocks = workDirectory.resolve("index/rocksdb");
        Path targetSqlite = workDirectory.resolve("index/sqlite");
        SnapshotIO.copyDirectory(sourceRocks, targetRocks);
        SnapshotIO.copyDirectory(sourceSqlite, targetSqlite);
        Path tableDb = targetSqlite.resolve(table.tableId + ".main.index.db");
        if (!Files.isRegularFile(tableDb))
        {
            throw new IOException("snapshot is missing the table MainIndex: " + tableDb);
        }

        applyConfig(INDEX_CONFIG);
        ConfigFactory pixels = ConfigFactory.Instance();
        Path stats = workDirectory.resolve("index/rocksdb-stats");
        Files.createDirectories(stats);
        pixels.addProperty("enabled.single.point.index.schemes", scheme);
        pixels.addProperty("enabled.main.index.scheme", "sqlite");
        pixels.addProperty("index.rocksdb.data.path", targetRocks.toString());
        pixels.addProperty("index.rocksdb.stats.path", stats.toString());
        pixels.addProperty("index.sqlite.path", targetSqlite.toString());
        pixels.addProperty("retina.upsert-mode.enabled", "false");
    }

    /**
     * Prepare a clean MainIndex directory for a freshly constructed
     * PixelsWriteBuffer fixture. Active MemTables and allocator batches are
     * intentionally not restorable process state, and the fixture receives a
     * new Metadata table ID, so copying the source table's SQLite DB would be
     * both incorrect and unsafe.
     */
    public void prepareFreshWriteBufferState() throws IOException
    {
        requireSemanticConfig("WriteBuffer", WRITE_BUFFER_REQUIRED_CONFIG);
        applyWriteBufferConfig();
        Path sqlite = workDirectory.resolve("write-buffer/sqlite");
        Path objects = workDirectory.resolve("write-buffer/objects");
        Files.createDirectories(sqlite);
        Files.createDirectories(objects);
        ConfigFactory pixels = ConfigFactory.Instance();
        pixels.addProperty("enabled.main.index.scheme", "sqlite");
        pixels.addProperty("index.sqlite.path", sqlite.toString());
        pixels.addProperty("retina.buffer.object.storage.folder", objects.toString());
    }

    public void applyVisibilityConfig() throws IOException
    {
        applyConfig(VISIBILITY_CONFIG);
        Path checkpoints = workDirectory.resolve("visibility/checkpoints");
        Files.createDirectories(checkpoints);
        ConfigFactory.Instance().addProperty("retina.checkpoint.dir",
                checkpoints.toUri().toString());
    }

    public void applyWriteBufferConfig()
    {
        applyConfig(WRITE_BUFFER_CONFIG);
    }

    private void applyConfig(Set<String> allowed)
    {
        ConfigFactory pixels = ConfigFactory.Instance();
        for (Map.Entry<String, String> entry : manifest.semanticConfig.entrySet())
        {
            if (allowed.contains(entry.getKey()))
            {
                pixels.addProperty(entry.getKey(), entry.getValue());
            }
        }
    }

    private void requireSemanticConfig(String module, String... keys) throws IOException
    {
        for (String key : keys)
        {
            String value = manifest.semanticConfig.get(key);
            if (value == null || value.trim().isEmpty())
            {
                throw new IOException(module + " snapshot is missing semanticConfig." + key);
            }
        }
    }

    public Path resolveArtifact(String relative) throws IOException
    {
        if (relative == null || relative.trim().isEmpty())
        {
            throw new IOException("snapshot artifact path is absent");
        }
        if (!manifest.artifactsSha256.containsKey(relative))
        {
            throw new IOException("snapshot artifact is not declared/checksummed: " + relative);
        }
        Path resolved = snapshotDirectory.resolve(relative).normalize();
        if (!resolved.startsWith(snapshotDirectory))
        {
            throw new IOException("snapshot artifact escapes its directory: " + relative);
        }
        if (!Files.isRegularFile(resolved) || Files.isSymbolicLink(resolved))
        {
            throw new IOException("snapshot artifact is not a regular file: " + resolved);
        }
        return resolved;
    }

    @Override
    public void close() throws IOException
    {
        if (ownsWorkDirectory)
        {
            deleteRecursively(workDirectory);
        }
    }

    private static boolean isEmptyDirectory(Path path) throws IOException
    {
        if (!Files.isDirectory(path))
        {
            return false;
        }
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(path))
        {
            return !entries.iterator().hasNext();
        }
    }

    private static void deleteRecursively(Path path) throws IOException
    {
        if (!Files.exists(path))
        {
            return;
        }
        Files.walkFileTree(path, new java.nio.file.SimpleFileVisitor<Path>()
        {
            @Override
            public java.nio.file.FileVisitResult visitFile(Path file,
                                                            java.nio.file.attribute.BasicFileAttributes attrs)
                    throws IOException
            {
                Files.delete(file);
                return java.nio.file.FileVisitResult.CONTINUE;
            }

            @Override
            public java.nio.file.FileVisitResult postVisitDirectory(Path dir, IOException error)
                    throws IOException
            {
                if (error != null) throw error;
                Files.delete(dir);
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
    }
}
