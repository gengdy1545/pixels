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

/** Prepares disposable physical state and effective configuration before Pixels singletons start. */
public final class SnapshotRuntime implements AutoCloseable
{
    private static final Set<String> WRITE_BUFFER_CONFIG = new HashSet<>(Arrays.asList(
            "pixel.stride", "row.group.size", "block.size", "block.replication",
            "column.chunk.little.endian", "column.chunk.alignment", "isnull.bitmap.alignment",
            "enabled.storage.schemes", "hdfs.config.dir",
            "s3.connection.timeout.sec", "s3.connection.acquisition.timeout.sec",
            "s3.client.service.threads", "s3.max.request.concurrency",
            "s3.max.pending.requests", "s3.enable.async", "s3.use.async.client",
            "minio.region", "minio.endpoint",
            "node.virtual.num", "index.main.cache.bucket.num",
            "retina.buffer.memTable.size", "retina.buffer.flush.count",
            "retina.buffer.object.flush.threads", "retina.buffer.flush.interval",
            "retina.buffer.flush.encodingLevel", "retina.buffer.flush.nullsPadding",
            "retina.buffer.object.storage.scheme", "retina.buffer.object.storage.folder",
            "retina.tile.visibility.capacity"));

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

        applyAllConfig();
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
        applyWriteBufferConfig();
        Path sqlite = workDirectory.resolve("write-buffer/sqlite");
        Files.createDirectories(sqlite);
        ConfigFactory pixels = ConfigFactory.Instance();
        pixels.addProperty("enabled.main.index.scheme", "sqlite");
        pixels.addProperty("index.sqlite.path", sqlite.toString());
    }

    public void applyAllConfig()
    {
        ConfigFactory pixels = ConfigFactory.Instance();
        for (Map.Entry<String, String> entry : manifest.effectiveConfig.entrySet())
        {
            pixels.addProperty(entry.getKey(), entry.getValue());
        }
    }

    public void applyWriteBufferConfig()
    {
        ConfigFactory pixels = ConfigFactory.Instance();
        for (Map.Entry<String, String> entry : manifest.effectiveConfig.entrySet())
        {
            if (WRITE_BUFFER_CONFIG.contains(entry.getKey()))
            {
                pixels.addProperty(entry.getKey(), entry.getValue());
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
