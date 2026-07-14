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

import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Reads a snapshot, validates its version and every declared SHA-256 artifact. */
public final class SnapshotValidator
{
    private SnapshotValidator()
    {
    }

    public static void run(BenchmarkConfig config) throws Exception
    {
        Path directory = Paths.get(config.require("snapshot-dir")).toAbsolutePath().normalize();
        SnapshotManifest manifest = SnapshotIO.readManifest(directory);
        Set<String> requiredProfiles = parseRequiredProfiles(
                config.get("snapshot-require", ""));
        List<SnapshotManifest.TableState> selectedTables = selectTables(
                manifest, config.get("snapshot-table", ""));
        validateRequiredProfiles(manifest, selectedTables, requiredProfiles);
        System.out.println("snapshot_directory=" + directory);
        System.out.println("format_version=" + manifest.formatVersion);
        System.out.println("source_host=" + manifest.sourceHost);
        System.out.println("source_quiesced=" + manifest.sourceQuiesced);
        System.out.println("physical_index_state=" + manifest.physicalIndexStateIncluded);
        System.out.println("snapshot_timestamp=" + manifest.snapshotTimestamp);
        System.out.println("checked_artifacts=" + manifest.artifactsSha256.size());
        System.out.println("required_profiles=" + (requiredProfiles.isEmpty()
                ? "none" : String.join(",", requiredProfiles)));
        System.out.println("tables=" + manifest.tables.size());
        for (SnapshotManifest.TableState table : manifest.tables)
        {
            System.out.println("table." + table.tableName + ".id=" + table.tableId);
            System.out.println("table." + table.tableName + ".files=" + table.files.size());
        }
        System.out.println("status=valid");
    }

    private static Set<String> parseRequiredProfiles(String value)
    {
        Set<String> profiles = new LinkedHashSet<>();
        if (value.trim().isEmpty())
        {
            return profiles;
        }
        for (String token : value.split(","))
        {
            String profile = token.trim().toLowerCase(Locale.ROOT);
            if (!"index".equals(profile) && !"visibility".equals(profile)
                    && !"write-buffer".equals(profile))
            {
                throw new IllegalArgumentException("unsupported --snapshot-require profile: "
                        + token + "; expected index, visibility, or write-buffer");
            }
            profiles.add(profile);
        }
        return profiles;
    }

    private static List<SnapshotManifest.TableState> selectTables(SnapshotManifest manifest,
                                                                   String tableName)
    {
        if (tableName.trim().isEmpty())
        {
            return manifest.tables;
        }
        List<SnapshotManifest.TableState> selected = new ArrayList<>();
        for (SnapshotManifest.TableState table : manifest.tables)
        {
            if (table.tableName.equals(tableName))
            {
                selected.add(table);
            }
        }
        if (selected.isEmpty())
        {
            throw new IllegalArgumentException("snapshot table not found: " + tableName);
        }
        return selected;
    }

    private static void validateRequiredProfiles(SnapshotManifest manifest,
                                                 List<SnapshotManifest.TableState> tables,
                                                 Set<String> profiles)
    {
        if (profiles.contains("index"))
        {
            if (!manifest.sourceQuiesced || !manifest.physicalIndexStateIncluded
                    || manifest.snapshotTimestamp <= 0)
            {
                throw new IllegalArgumentException("index profile requires sourceQuiesced=true, "
                        + "a positive T_snap, and complete physical RocksDB/SQLite state");
            }
            for (SnapshotManifest.TableState table : tables)
            {
                if (table.primaryIndex == null)
                {
                    throw new IllegalArgumentException("index profile lacks a primary-index descriptor for "
                            + table.schemaName + "." + table.tableName);
                }
            }
            requireSemanticConfig(manifest, "index", "index.bucket.num",
                    "index.rocksdb.multicf");
        }
        if (profiles.contains("visibility"))
        {
            for (SnapshotManifest.TableState table : tables)
            {
                if (table.files.isEmpty())
                {
                    throw new IllegalArgumentException("visibility profile lacks file/RG topology for "
                            + table.schemaName + "." + table.tableName);
                }
            }
        }
        if (profiles.contains("write-buffer"))
        {
            requireSemanticConfig(manifest, "write-buffer",
                    "retina.buffer.memTable.size", "retina.buffer.flush.count",
                    "retina.buffer.object.flush.threads", "retina.buffer.flush.interval",
                    "retina.buffer.flush.encodingLevel", "retina.buffer.flush.nullsPadding",
                    "retina.buffer.object.storage.scheme", "block.size", "block.replication",
                    "node.virtual.num", "index.main.cache.bucket.num",
                    "retina.tile.visibility.capacity", "enabled.storage.schemes");
            for (SnapshotManifest.TableState table : tables)
            {
                if (table.columns == null || table.columns.isEmpty())
                {
                    throw new IllegalArgumentException("write-buffer profile lacks a schema for "
                            + table.schemaName + "." + table.tableName);
                }
            }
        }
    }

    private static void requireSemanticConfig(SnapshotManifest manifest, String profile,
                                              String... keys)
    {
        List<String> missing = new ArrayList<>();
        for (String key : keys)
        {
            String value = manifest.semanticConfig.get(key);
            if (value == null || value.trim().isEmpty())
            {
                missing.add(key);
            }
        }
        if (!missing.isEmpty())
        {
            throw new IllegalArgumentException(profile
                    + " profile lacks semantic configuration keys " + missing);
        }
    }
}
