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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotSamples.IndexSample;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotSamples.RowSample;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Reads, writes and validates the versioned Retina benchmark snapshot. */
public final class SnapshotIO
{
    private static final int INDEX_MAGIC = 0x50495849; // PIXI
    private static final int ROW_MAGIC = 0x50495852;   // PIXR
    private static final int BINARY_VERSION = 1;
    private static final long MAX_MANIFEST_BYTES = 64L * 1024L * 1024L;

    private SnapshotIO()
    {
    }

    @FunctionalInterface
    public interface IndexSampleVisitor
    {
        /** Return false to stop after this sample. */
        boolean visit(IndexSample sample) throws Exception;
    }

    public static void writeManifest(Path snapshotDirectory, SnapshotManifest manifest) throws IOException
    {
        Files.createDirectories(snapshotDirectory);
        validateManifest(manifest);
        String json = JSON.toJSONString(manifest, SerializerFeature.PrettyFormat,
                SerializerFeature.SortField, SerializerFeature.WriteMapNullValue);
        Path target = snapshotDirectory.resolve(SnapshotManifest.FILE_NAME);
        Path staging = stagingPath(target);
        try
        {
            Files.write(staging, (json + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
            commitStaged(staging, target);
        }
        finally
        {
            Files.deleteIfExists(staging);
        }
    }

    public static SnapshotManifest readManifest(Path snapshotDirectory) throws IOException
    {
        Path root = snapshotDirectory.toAbsolutePath().normalize();
        Path path = root.resolve(SnapshotManifest.FILE_NAME);
        if (!Files.isRegularFile(path))
        {
            throw new IOException("snapshot manifest does not exist: " + path);
        }
        if (Files.size(path) > MAX_MANIFEST_BYTES)
        {
            throw new IOException("snapshot manifest exceeds " + MAX_MANIFEST_BYTES + " bytes: " + path);
        }
        SnapshotManifest manifest = JSON.parseObject(
                new String(Files.readAllBytes(path), StandardCharsets.UTF_8), SnapshotManifest.class);
        validateManifest(manifest);
        verifyArtifacts(root, manifest);
        return manifest;
    }

    public static SnapshotManifest.TableState requireTable(SnapshotManifest manifest, String tableName)
    {
        SnapshotManifest.TableState found = null;
        for (SnapshotManifest.TableState table : manifest.tables)
        {
            if (table.tableName.equalsIgnoreCase(tableName)
                    || (table.schemaName + "." + table.tableName).equalsIgnoreCase(tableName))
            {
                if (found != null)
                {
                    throw new IllegalArgumentException("snapshot table name is ambiguous: " + tableName);
                }
                found = table;
            }
        }
        if (found == null)
        {
            throw new IllegalArgumentException("table is not present in snapshot: " + tableName);
        }
        return found;
    }

    public static void writeIndexSamples(Path path, List<IndexSample> samples) throws IOException
    {
        Files.createDirectories(path.toAbsolutePath().getParent());
        Path staging = stagingPath(path);
        try
        {
            try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(staging))))
            {
                out.writeInt(INDEX_MAGIC);
                out.writeInt(BINARY_VERSION);
                out.writeLong(samples.size());
                for (IndexSample sample : samples)
                {
                    if (sample.key == null || sample.key.length == 0)
                    {
                        throw new IOException("index sample contains an empty key");
                    }
                    out.writeInt(sample.key.length);
                    out.write(sample.key);
                    out.writeLong(sample.createTimestamp);
                    out.writeLong(sample.fileId);
                    out.writeInt(sample.rgId);
                    out.writeInt(sample.rgRowOffset);
                    out.writeInt(sample.bucketId);
                }
            }
            commitStaged(staging, path);
        }
        finally
        {
            Files.deleteIfExists(staging);
        }
    }

    public static List<IndexSample> readIndexSamples(Path path, long limit) throws IOException
    {
        if (limit < 0 || limit > Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException("invalid index sample read limit: " + limit);
        }
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path))))
        {
            requireHeader(in, INDEX_MAGIC, path);
            long count = in.readLong();
            if (count < 0 || count > Integer.MAX_VALUE)
            {
                throw new IOException("invalid index sample count " + count + " in " + path);
            }
            int toRead = (int) Math.min(count, limit == 0 ? count : limit);
            List<IndexSample> samples = new ArrayList<>(toRead);
            for (int i = 0; i < count; i++)
            {
                int keyLength = in.readInt();
                if (keyLength <= 0 || keyLength > 1024 * 1024)
                {
                    throw new IOException("invalid index key length " + keyLength + " in " + path);
                }
                byte[] key = null;
                if (i < toRead)
                {
                    key = new byte[keyLength];
                    in.readFully(key);
                }
                else
                {
                    skipFully(in, keyLength);
                }
                long timestamp = in.readLong();
                long fileId = in.readLong();
                int rgId = in.readInt();
                int offset = in.readInt();
                int bucket = in.readInt();
                if (key != null)
                {
                    samples.add(new IndexSample(key, timestamp, fileId, rgId, offset, bucket));
                }
            }
            rejectTrailingBytes(in, path);
            return samples;
        }
        catch (EOFException e)
        {
            throw new IOException("truncated index sample file: " + path, e);
        }
    }

    /** Fully scans an index sample artifact without retaining its payload. */
    public static long validateIndexSamples(Path path) throws IOException
    {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path))))
        {
            requireHeader(in, INDEX_MAGIC, path);
            long count = in.readLong();
            if (count < 0 || count > Integer.MAX_VALUE)
            {
                throw new IOException("invalid index sample count " + count + " in " + path);
            }
            for (long i = 0; i < count; i++)
            {
                int keyLength = in.readInt();
                if (keyLength <= 0 || keyLength > 1024 * 1024)
                {
                    throw new IOException("invalid index key length " + keyLength + " in " + path);
                }
                skipFully(in, keyLength);
                long timestamp = in.readLong();
                long fileId = in.readLong();
                int rgId = in.readInt();
                int offset = in.readInt();
                int bucket = in.readInt();
                if (timestamp < 0 || fileId <= 0 || rgId < 0 || offset < 0 || bucket < 0)
                {
                    throw new IOException("invalid index sample at ordinal " + i + " in " + path);
                }
            }
            rejectTrailingBytes(in, path);
            return count;
        }
        catch (EOFException e)
        {
            throw new IOException("truncated index sample file: " + path, e);
        }
    }

    /** Streams index samples in file order and retains no sample collection. */
    public static long scanIndexSamples(Path path, IndexSampleVisitor visitor) throws Exception
    {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path))))
        {
            requireHeader(in, INDEX_MAGIC, path);
            long count = in.readLong();
            if (count < 0 || count > Integer.MAX_VALUE)
            {
                throw new IOException("invalid index sample count " + count + " in " + path);
            }
            long visited = 0L;
            for (long i = 0; i < count; i++)
            {
                int keyLength = in.readInt();
                if (keyLength <= 0 || keyLength > 1024 * 1024)
                {
                    throw new IOException("invalid index key length " + keyLength + " in " + path);
                }
                byte[] key = new byte[keyLength];
                in.readFully(key);
                IndexSample sample = new IndexSample(key, in.readLong(), in.readLong(),
                        in.readInt(), in.readInt(), in.readInt());
                visited++;
                if (!visitor.visit(sample))
                {
                    return visited;
                }
            }
            rejectTrailingBytes(in, path);
            return visited;
        }
        catch (EOFException e)
        {
            throw new IOException("truncated index sample file: " + path, e);
        }
    }

    public static void writeRowSamples(Path path, List<RowSample> samples, int columnCount) throws IOException
    {
        Files.createDirectories(path.toAbsolutePath().getParent());
        Path staging = stagingPath(path);
        try
        {
            try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(staging))))
            {
                out.writeInt(ROW_MAGIC);
                out.writeInt(BINARY_VERSION);
                out.writeInt(columnCount);
                out.writeLong(samples.size());
                for (RowSample sample : samples)
                {
                    if (sample.columns == null || sample.columns.length != columnCount)
                    {
                        throw new IOException("row sample column count mismatch");
                    }
                    for (byte[] value : sample.columns)
                    {
                        if (value == null)
                        {
                            out.writeInt(-1);
                        }
                        else
                        {
                            out.writeInt(value.length);
                            out.write(value);
                        }
                    }
                }
            }
            commitStaged(staging, path);
        }
        finally
        {
            Files.deleteIfExists(staging);
        }
    }

    public static List<RowSample> readRowSamples(Path path, int expectedColumns, long limit) throws IOException
    {
        if (limit < 0 || limit > Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException("invalid row sample read limit: " + limit);
        }
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path))))
        {
            requireHeader(in, ROW_MAGIC, path);
            int columns = in.readInt();
            if (columns != expectedColumns)
            {
                throw new IOException("row sample has " + columns + " columns, expected "
                        + expectedColumns + ": " + path);
            }
            long count = in.readLong();
            if (count < 0 || count > Integer.MAX_VALUE)
            {
                throw new IOException("invalid row sample count " + count + " in " + path);
            }
            int toRead = (int) Math.min(count, limit == 0 ? count : limit);
            List<RowSample> samples = new ArrayList<>(toRead);
            for (int row = 0; row < count; row++)
            {
                byte[][] values = row < toRead ? new byte[columns][] : null;
                for (int column = 0; column < columns; column++)
                {
                    int length = in.readInt();
                    if (length < -1 || length > 256 * 1024 * 1024)
                    {
                        throw new IOException("invalid row value length " + length + " in " + path);
                    }
                    if (length >= 0)
                    {
                        if (values != null)
                        {
                            byte[] value = new byte[length];
                            in.readFully(value);
                            values[column] = value;
                        }
                        else
                        {
                            skipFully(in, length);
                        }
                    }
                }
                if (values != null)
                {
                    samples.add(new RowSample(values));
                }
            }
            rejectTrailingBytes(in, path);
            return samples;
        }
        catch (EOFException e)
        {
            throw new IOException("truncated row sample file: " + path, e);
        }
    }

    /** Fully scans a row sample artifact without retaining its payload. */
    public static long validateRowSamples(Path path, int expectedColumns) throws IOException
    {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path))))
        {
            requireHeader(in, ROW_MAGIC, path);
            int columns = in.readInt();
            if (columns != expectedColumns)
            {
                throw new IOException("row sample has " + columns + " columns, expected "
                        + expectedColumns + ": " + path);
            }
            long count = in.readLong();
            if (count < 0 || count > Integer.MAX_VALUE)
            {
                throw new IOException("invalid row sample count " + count + " in " + path);
            }
            for (long row = 0; row < count; row++)
            {
                for (int column = 0; column < columns; column++)
                {
                    int length = in.readInt();
                    if (length < -1 || length > 256 * 1024 * 1024)
                    {
                        throw new IOException("invalid row value length " + length + " in " + path);
                    }
                    if (length >= 0)
                    {
                        skipFully(in, length);
                    }
                }
            }
            rejectTrailingBytes(in, path);
            return count;
        }
        catch (EOFException e)
        {
            throw new IOException("truncated row sample file: " + path, e);
        }
    }

    public static String sha256(Path path) throws IOException
    {
        MessageDigest digest;
        try
        {
            digest = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
        byte[] buffer = new byte[1024 * 1024];
        try (BufferedInputStream input = new BufferedInputStream(Files.newInputStream(path)))
        {
            int count;
            while ((count = input.read(buffer)) >= 0)
            {
                if (count > 0)
                {
                    digest.update(buffer, 0, count);
                }
            }
        }
        Formatter formatter = new Formatter();
        try
        {
            for (byte value : digest.digest())
            {
                formatter.format("%02x", value);
            }
            return formatter.toString();
        }
        finally
        {
            formatter.close();
        }
    }

    public static void copyDirectory(Path source, Path target) throws IOException
    {
        source = source.toAbsolutePath().normalize();
        target = target.toAbsolutePath().normalize();
        if (!Files.isDirectory(source))
        {
            throw new IOException("snapshot state directory does not exist: " + source);
        }
        if (target.startsWith(source) || source.startsWith(target))
        {
            throw new IOException("snapshot copy source and target must not contain each other: "
                    + source + " -> " + target);
        }
        final Path sourceRoot = source;
        final Path targetRoot = target;
        Files.walkFileTree(source, new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException
            {
                Files.createDirectories(targetRoot.resolve(sourceRoot.relativize(dir)));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
            {
                if (!attrs.isRegularFile() || Files.isSymbolicLink(file))
                {
                    throw new IOException("snapshot state contains a symbolic link or special file: " + file);
                }
                Files.copy(file, targetRoot.resolve(sourceRoot.relativize(file)),
                        StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /** Adds every regular file below a copied state directory to the manifest checksum set. */
    public static void addDirectoryChecksums(Path snapshotDirectory, Path directory,
                                             Map<String, String> checksums) throws IOException
    {
        final Path root = snapshotDirectory.toAbsolutePath().normalize();
        final Path state = directory.toAbsolutePath().normalize();
        if (!state.startsWith(root))
        {
            throw new IOException("state directory is outside the snapshot: " + state);
        }
        Files.walkFileTree(state, new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
            {
                if (attrs.isRegularFile())
                {
                    String relative = root.relativize(file.toAbsolutePath().normalize()).toString();
                    checksums.put(relative, sha256(file));
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static void validateManifest(SnapshotManifest manifest)
    {
        if (manifest == null)
        {
            throw new IllegalArgumentException("snapshot manifest is null");
        }
        if (manifest.formatVersion != SnapshotManifest.FORMAT_VERSION)
        {
            throw new IllegalArgumentException("unsupported snapshot format version: "
                    + manifest.formatVersion);
        }
        if (manifest.tables == null || manifest.tables.isEmpty())
        {
            throw new IllegalArgumentException("snapshot contains no tables");
        }
        if (manifest.sourceHost == null || manifest.sourceHost.trim().isEmpty())
        {
            throw new IllegalArgumentException("snapshot sourceHost is empty");
        }
        if (manifest.sourceVnodeIds == null || manifest.sourceIndexBucketIds == null
                || manifest.effectiveConfig == null || manifest.artifactsSha256 == null)
        {
            throw new IllegalArgumentException("snapshot contains a null top-level collection");
        }
        if ((manifest.physicalIndexStateIncluded || manifest.visibilityCheckpointIncluded)
                && !manifest.sourceQuiesced)
        {
            throw new IllegalArgumentException("physical snapshot state requires sourceQuiesced=true");
        }
        boolean hasRocksDbArtifact = hasArtifactBelow(manifest, "state/index/rocksdb/");
        boolean hasSqliteArtifact = hasArtifactBelow(manifest, "state/index/sqlite/");
        if ((manifest.physicalIndexStateIncluded
                && !(hasRocksDbArtifact && hasSqliteArtifact))
                || (!manifest.physicalIndexStateIncluded
                && (hasRocksDbArtifact || hasSqliteArtifact)))
        {
            throw new IllegalArgumentException("physical Index state/artifact descriptor mismatch");
        }
        if (manifest.visibilityCheckpointIncluded)
        {
            if (manifest.snapshotTimestamp <= 0
                    || manifest.visibilityCheckpointSource == null
                    || manifest.visibilityCheckpointTimestamp != manifest.snapshotTimestamp
                    || manifest.visibilityCheckpointHost == null
                    || manifest.visibilityCheckpointHost.trim().isEmpty()
                    || !("gc".equals(manifest.visibilityCheckpointType)
                    || "offload".equals(manifest.visibilityCheckpointType))
                    || !manifest.artifactsSha256.containsKey(
                    "state/visibility/gc-checkpoint.bin")
                    || manifest.visibilityCheckpointEntryCount < 0
                    || manifest.visibilityCheckpointExpectedEntryCount <= 0
                    || manifest.visibilityCheckpointMatchedEntryCount < 0
                    || manifest.visibilityCheckpointUnmatchedEntryCount < 0
                    || manifest.visibilityCheckpointMatchedEntryCount
                    + manifest.visibilityCheckpointUnmatchedEntryCount
                    != manifest.visibilityCheckpointEntryCount
                    || manifest.visibilityCheckpointMatchedEntryCount
                    != manifest.visibilityCheckpointExpectedEntryCount)
            {
                throw new IllegalArgumentException("invalid Visibility checkpoint descriptor");
            }
        }
        else if (manifest.visibilityCheckpointSource != null
                || manifest.visibilityCheckpointType != null
                || manifest.visibilityCheckpointHost != null
                || manifest.visibilityCheckpointTimestamp != 0
                || manifest.visibilityCheckpointEntryCount != 0
                || manifest.visibilityCheckpointExpectedEntryCount != 0
                || manifest.visibilityCheckpointMatchedEntryCount != 0
                || manifest.visibilityCheckpointUnmatchedEntryCount != 0)
        {
            throw new IllegalArgumentException("snapshot has checkpoint metadata but no checkpoint");
        }
        if (manifest.visibilityCheckpointIncluded
                != manifest.artifactsSha256.containsKey("state/visibility/gc-checkpoint.bin"))
        {
            throw new IllegalArgumentException("Visibility checkpoint/artifact descriptor mismatch");
        }
        validateNonNegativeUniqueIds(manifest.sourceVnodeIds, "sourceVnodeIds");
        validateNonNegativeUniqueIds(manifest.sourceIndexBucketIds, "sourceIndexBucketIds");
        Set<Long> tableIds = new HashSet<>();
        Set<String> tableNames = new HashSet<>();
        for (SnapshotManifest.TableState table : manifest.tables)
        {
            if (table.schemaName == null || table.tableName == null || table.tableId <= 0
                    || table.columns == null || table.columns.isEmpty()
                    || table.layouts == null || table.files == null)
            {
                throw new IllegalArgumentException("snapshot contains an invalid table entry");
            }
            if (!tableIds.add(table.tableId)
                    || !tableNames.add((table.schemaName + "." + table.tableName).toLowerCase()))
            {
                throw new IllegalArgumentException("snapshot contains a duplicate table: "
                        + table.schemaName + "." + table.tableName);
            }
            Set<Long> columnIds = new HashSet<>();
            for (int i = 0; i < table.columns.size(); i++)
            {
                SnapshotManifest.ColumnState column = table.columns.get(i);
                if (column == null || column.ordinal != i || column.columnId <= 0
                        || column.name == null || column.type == null || !columnIds.add(column.columnId))
                {
                    throw new IllegalArgumentException("invalid/duplicate column in snapshot table "
                            + table.tableName + " at ordinal " + i);
                }
            }
            Set<Long> layoutIds = new HashSet<>();
            Set<Long> pathIds = new HashSet<>();
            for (SnapshotManifest.LayoutState layout : table.layouts)
            {
                if (layout == null || layout.paths == null || layout.layoutId <= 0
                        || layout.tableId != table.tableId
                        || !layoutIds.add(layout.layoutId))
                {
                    throw new IllegalArgumentException("invalid/duplicate layout in snapshot table "
                            + table.tableName);
                }
                for (SnapshotManifest.PathState path : layout.paths)
                {
                    if (path == null || path.pathId <= 0 || path.uri == null || path.role == null
                            || !pathIds.add(path.pathId))
                    {
                        throw new IllegalArgumentException("invalid/duplicate path in snapshot table "
                                + table.tableName);
                    }
                }
            }
            if (table.selectedSampleLayoutId > 0 && !layoutIds.contains(table.selectedSampleLayoutId))
            {
                throw new IllegalArgumentException("sample layout is absent from snapshot table "
                        + table.tableName);
            }
            Set<Long> fileIds = new HashSet<>();
            for (SnapshotManifest.FileState file : table.files)
            {
                if (file == null || file.rowGroups == null || file.fileId <= 0
                        || !fileIds.add(file.fileId)
                        || !layoutIds.contains(file.layoutId) || !pathIds.contains(file.pathId)
                        || file.fullUri == null || file.footerRowCount < 0
                        || file.footerRowGroupCount != file.rowGroups.size())
                {
                    throw new IllegalArgumentException("invalid/duplicate Pixels file in snapshot table "
                            + table.tableName);
                }
                long rows = 0;
                for (int rgId = 0; rgId < file.rowGroups.size(); rgId++)
                {
                    SnapshotManifest.RowGroupState rg = file.rowGroups.get(rgId);
                    if (rg == null || rg.rgId != rgId || rg.recordNum < 0)
                    {
                        throw new IllegalArgumentException("invalid row group for file " + file.fileId);
                    }
                    rows += rg.recordNum;
                }
                if (rows != file.footerRowCount)
                {
                    throw new IllegalArgumentException("row count mismatch for file " + file.fileId);
                }
            }
            if (table.indexes == null)
            {
                throw new IllegalArgumentException("snapshot index descriptor list is null for "
                        + table.tableName);
            }
            Set<Long> indexIds = new HashSet<>();
            boolean primaryFound = false;
            for (SnapshotManifest.IndexState index : table.indexes)
            {
                if (index == null || index.keyColumnIds == null
                        || index.keyColumnOrdinals == null || index.keyColumnNames == null
                        || index.keyColumnTypes == null)
                {
                    throw new IllegalArgumentException("null index descriptor in snapshot table "
                            + table.tableName);
                }
                int keyCount = index.keyColumnIds.size();
                if (index.indexId <= 0 || !indexIds.add(index.indexId) || index.scheme == null
                        || keyCount == 0 || index.keyColumnOrdinals.size() != keyCount
                        || index.keyColumnNames.size() != keyCount
                        || index.keyColumnTypes.size() != keyCount
                        || !validIndexLength(index.canonicalKeyBytes)
                        || !validIndexLength(index.rocksDbPrefixKeyBytes))
                {
                    throw new IllegalArgumentException("invalid/duplicate index in snapshot table "
                            + table.tableName);
                }
                for (Integer ordinal : index.keyColumnOrdinals)
                {
                    if (ordinal == null || ordinal < 0 || ordinal >= table.columns.size())
                    {
                        throw new IllegalArgumentException("invalid primary-key ordinal in " + table.tableName);
                    }
                }
                if (index.primary)
                {
                    if (primaryFound || !index.unique)
                    {
                        throw new IllegalArgumentException("invalid primary index set in " + table.tableName);
                    }
                    primaryFound = true;
                }
            }
            if ((table.primaryIndex == null) != !primaryFound
                    || (table.primaryIndex != null && !indexIds.contains(table.primaryIndex.indexId)))
            {
                throw new IllegalArgumentException("primary index reference mismatch in " + table.tableName);
            }
            validateSampleReference(manifest, table.indexSamplesFile, table.indexSampleCount,
                    "index", table.tableName);
            validateSampleReference(manifest, table.rowSamplesFile, table.rowSampleCount,
                    "row", table.tableName);
            if (table.indexSampleCount > 0 && (manifest.snapshotTimestamp <= 0
                    || table.maxObservedCreateTimestamp > manifest.snapshotTimestamp))
            {
                throw new IllegalArgumentException("index samples exceed or lack the authoritative "
                        + "snapshot timestamp for " + table.tableName);
            }
        }
        if (manifest.visibilityCheckpointIncluded
                && countProductionVisibilityRowGroups(manifest)
                != manifest.visibilityCheckpointExpectedEntryCount)
        {
            throw new IllegalArgumentException("Visibility checkpoint expected-RG count does not "
                    + "match the manifest production topology");
        }
    }

    private static void validateSampleReference(SnapshotManifest manifest, String relative, long count,
                                                String type, String table)
    {
        if (count < 0)
        {
            throw new IllegalArgumentException(type + " sample count is negative for " + table);
        }
        if ((relative == null) != (count == 0))
        {
            throw new IllegalArgumentException(type + " sample reference/count mismatch for " + table);
        }
        if (relative != null && (manifest.artifactsSha256 == null
                || !manifest.artifactsSha256.containsKey(relative)))
        {
            throw new IllegalArgumentException(type + " sample is not checksummed for " + table);
        }
    }

    private static boolean validIndexLength(int length)
    {
        return length == -1 || length > 0;
    }

    private static void validateNonNegativeUniqueIds(List<Integer> values, String name)
    {
        Set<Integer> unique = new HashSet<>();
        for (Integer value : values)
        {
            if (value == null || value < 0 || !unique.add(value))
            {
                throw new IllegalArgumentException("invalid/duplicate " + name + " value: " + value);
            }
        }
    }

    private static long countProductionVisibilityRowGroups(SnapshotManifest manifest)
    {
        long count = 0L;
        for (SnapshotManifest.TableState table : manifest.tables)
        {
            Set<Long> selectedPaths = new HashSet<>();
            for (SnapshotManifest.LayoutState layout : table.layouts)
            {
                if (!layout.readable)
                {
                    continue;
                }
                for (SnapshotManifest.PathState path : layout.paths)
                {
                    if (path.productionSelectedByRetina
                            && ("ordered".equalsIgnoreCase(path.role)
                            || "compact".equalsIgnoreCase(path.role)))
                    {
                        selectedPaths.add(path.pathId);
                    }
                }
            }
            for (SnapshotManifest.FileState file : table.files)
            {
                if (selectedPaths.contains(file.pathId))
                {
                    count = Math.addExact(count, file.rowGroups.size());
                }
            }
        }
        return count;
    }

    private static boolean hasArtifactBelow(SnapshotManifest manifest, String prefix)
    {
        for (String path : manifest.artifactsSha256.keySet())
        {
            if (path != null && path.replace('\\', '/').startsWith(prefix))
            {
                return true;
            }
        }
        return false;
    }

    private static void verifyArtifacts(Path snapshotDirectory, SnapshotManifest manifest) throws IOException
    {
        if (manifest.artifactsSha256 == null)
        {
            return;
        }
        for (java.util.Map.Entry<String, String> artifact : manifest.artifactsSha256.entrySet())
        {
            String relative = artifact.getKey();
            if (relative == null || relative.isEmpty() || Paths.get(relative).isAbsolute()
                    || artifact.getValue() == null || !artifact.getValue().matches("[0-9a-fA-F]{64}"))
            {
                throw new IOException("invalid snapshot artifact entry: " + relative);
            }
            Path path = snapshotDirectory.resolve(relative).normalize();
            if (!path.startsWith(snapshotDirectory))
            {
                throw new IOException("snapshot artifact escapes its directory: " + artifact.getKey());
            }
            if (!Files.isRegularFile(path))
            {
                throw new IOException("snapshot artifact is missing: " + path);
            }
            Path cursor = snapshotDirectory;
            for (Path part : snapshotDirectory.relativize(path))
            {
                cursor = cursor.resolve(part);
                if (Files.isSymbolicLink(cursor))
                {
                    throw new IOException("symbolic links are not allowed in snapshot artifacts: " + path);
                }
            }
            String actual = sha256(path);
            if (!actual.equalsIgnoreCase(artifact.getValue()))
            {
                throw new IOException("snapshot artifact checksum mismatch: " + path);
            }
        }
    }

    private static void requireHeader(DataInputStream in, int expectedMagic, Path path) throws IOException
    {
        int magic = in.readInt();
        int version = in.readInt();
        if (magic != expectedMagic || version != BINARY_VERSION)
        {
            throw new IOException("invalid sample header in " + path + ": magic="
                    + Integer.toHexString(magic) + ", version=" + version);
        }
    }

    private static void rejectTrailingBytes(DataInputStream in, Path path) throws IOException
    {
        if (in.read() != -1)
        {
            throw new IOException("unexpected trailing bytes in sample file: " + path);
        }
    }

    private static void skipFully(DataInputStream in, int length) throws IOException
    {
        int remaining = length;
        while (remaining > 0)
        {
            int skipped = in.skipBytes(remaining);
            if (skipped <= 0)
            {
                if (in.read() < 0)
                {
                    throw new EOFException("truncated while skipping " + length + " bytes");
                }
                skipped = 1;
            }
            remaining -= skipped;
        }
    }

    private static Path stagingPath(Path target)
    {
        Path absolute = target.toAbsolutePath();
        return absolute.resolveSibling(absolute.getFileName() + ".tmp-" + UUID.randomUUID());
    }

    private static void commitStaged(Path staging, Path target) throws IOException
    {
        try
        {
            Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        }
        catch (AtomicMoveNotSupportedException ignored)
        {
            Files.move(staging, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
