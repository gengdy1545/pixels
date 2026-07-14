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
import java.io.BufferedInputStream;
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
import java.util.HashSet;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Reads, writes and validates the versioned Retina benchmark snapshot. */
public final class SnapshotIO
{
    private static final long MAX_MANIFEST_BYTES = 64L * 1024L * 1024L;

    private SnapshotIO()
    {
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
        if (manifest.snapshotTimestamp < 0)
        {
            throw new IllegalArgumentException("snapshotTimestamp is negative");
        }
        if (manifest.sourceVnodeIds == null || manifest.sourceIndexBucketIds == null
                || manifest.semanticConfig == null || manifest.artifactsSha256 == null)
        {
            throw new IllegalArgumentException("snapshot contains a null top-level collection");
        }
        if (manifest.physicalIndexStateIncluded && !manifest.sourceQuiesced)
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
        if (hasArtifactBelow(manifest, "state/visibility/"))
        {
            throw new IllegalArgumentException("Snapshot v2 clean Visibility baseline does not "
                    + "support state/visibility artifacts");
        }
        validateNonNegativeUniqueIds(manifest.sourceVnodeIds, "sourceVnodeIds");
        validateNonNegativeUniqueIds(manifest.sourceIndexBucketIds, "sourceIndexBucketIds");
        validateSemanticConfig(manifest.semanticConfig);
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
        }
    }

    private static boolean validIndexLength(int length)
    {
        return length == -1 || length > 0;
    }

    private static void validateSemanticConfig(Map<String, String> config)
    {
        for (Map.Entry<String, String> entry : config.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key == null || key.trim().isEmpty() || value == null)
            {
                throw new IllegalArgumentException("invalid null/empty semantic configuration entry");
            }
            String normalized = key.toLowerCase();
            if (normalized.endsWith(".host") || normalized.endsWith(".hosts")
                    || normalized.endsWith(".port") || normalized.endsWith(".path")
                    || normalized.endsWith(".dir") || normalized.endsWith(".folder")
                    || normalized.contains("endpoint") || normalized.contains("credential")
                    || normalized.contains("password") || normalized.contains("secret")
                    || normalized.contains("token") || normalized.contains("access.key"))
            {
                throw new IllegalArgumentException("runtime endpoint, credential or path is not "
                        + "allowed in semanticConfig: " + key);
            }
        }
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
            String normalizedRelative = relative.replace('\\', '/');
            if (normalizedRelative.endsWith("/index-samples.bin")
                    || normalizedRelative.endsWith("/row-samples.bin")
                    || "index-samples.bin".equals(normalizedRelative)
                    || "row-samples.bin".equals(normalizedRelative))
            {
                throw new IOException("Snapshot v2 does not support sample artifacts: " + relative);
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
