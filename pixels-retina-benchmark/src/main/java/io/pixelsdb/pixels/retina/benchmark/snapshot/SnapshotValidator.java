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

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.utils.IndexUtils;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
        System.out.println("visibility_checkpoint=" + manifest.visibilityCheckpointIncluded);
        if (manifest.visibilityCheckpointIncluded)
        {
            System.out.println("visibility_checkpoint_type=" + manifest.visibilityCheckpointType);
            System.out.println("visibility_checkpoint_host=" + manifest.visibilityCheckpointHost);
            System.out.println("visibility_checkpoint_timestamp="
                    + manifest.visibilityCheckpointTimestamp);
            System.out.println("visibility_checkpoint_coverage="
                    + manifest.visibilityCheckpointMatchedEntryCount + "/"
                    + manifest.visibilityCheckpointExpectedEntryCount);
        }
        System.out.println("checked_artifacts=" + manifest.artifactsSha256.size());
        System.out.println("required_profiles=" + (requiredProfiles.isEmpty()
                ? "none" : String.join(",", requiredProfiles)));
        System.out.println("tables=" + manifest.tables.size());
        for (SnapshotManifest.TableState table : manifest.tables)
        {
            if (table.indexSamplesFile != null)
            {
                long count = validateIndexSampleSemantics(directory, manifest, table);
                if (count != table.indexSampleCount)
                {
                    throw new IllegalArgumentException("index sample count mismatch for "
                            + table.tableName + ": " + count + " != " + table.indexSampleCount);
                }
            }
            if (table.rowSamplesFile != null)
            {
                long count = SnapshotIO.validateRowSamples(
                        directory.resolve(table.rowSamplesFile), table.columns.size());
                if (count != table.rowSampleCount)
                {
                    throw new IllegalArgumentException("row sample count mismatch for "
                            + table.tableName + ": " + count + " != " + table.rowSampleCount);
                }
            }
            System.out.println("table." + table.tableName + ".id=" + table.tableId);
            System.out.println("table." + table.tableName + ".files=" + table.files.size());
            System.out.println("table." + table.tableName + ".index_samples=" + table.indexSampleCount);
            System.out.println("table." + table.tableName + ".row_samples=" + table.rowSampleCount);
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
                    && !"visibility-clean".equals(profile)
                    && !"write-buffer".equals(profile))
            {
                throw new IllegalArgumentException("unsupported --snapshot-require profile: "
                        + token + "; expected index, visibility, visibility-clean, or write-buffer");
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
                if (table.primaryIndex == null || table.indexSamplesFile == null
                        || table.indexSampleCount <= 0)
                {
                    throw new IllegalArgumentException("index profile lacks primary-index samples for "
                            + table.schemaName + "." + table.tableName);
                }
            }
        }
        if (profiles.contains("visibility"))
        {
            if (!manifest.sourceQuiesced || manifest.snapshotTimestamp <= 0
                    || !manifest.visibilityCheckpointIncluded)
            {
                throw new IllegalArgumentException("visibility profile requires sourceQuiesced=true, "
                        + "a positive T_snap, and a full visibility checkpoint");
            }
        }
        if (profiles.contains("visibility-clean"))
        {
            for (SnapshotManifest.TableState table : tables)
            {
                if (table.files.isEmpty())
                {
                    throw new IllegalArgumentException("visibility-clean profile lacks file/RG topology for "
                            + table.schemaName + "." + table.tableName);
                }
            }
        }
        if (profiles.contains("write-buffer"))
        {
            for (SnapshotManifest.TableState table : tables)
            {
                if (table.rowSamplesFile == null || table.rowSampleCount <= 0)
                {
                    throw new IllegalArgumentException("write-buffer profile lacks real row samples for "
                            + table.schemaName + "." + table.tableName);
                }
            }
        }
    }

    private static long validateIndexSampleSemantics(Path directory, SnapshotManifest manifest,
                                                     SnapshotManifest.TableState table)
            throws Exception
    {
        if (table.primaryIndex == null)
        {
            throw new IllegalArgumentException("index samples have no primary index descriptor for "
                    + table.tableName);
        }
        String configuredBuckets = manifest.effectiveConfig.get("index.bucket.num");
        if (configuredBuckets == null)
        {
            throw new IllegalArgumentException("snapshot effectiveConfig lacks index.bucket.num");
        }
        int bucketCount = Integer.parseInt(configuredBuckets);
        if (bucketCount <= 0)
        {
            throw new IllegalArgumentException("invalid snapshot index.bucket.num=" + bucketCount);
        }
        Map<Long, Map<Integer, Integer>> topology = new HashMap<>();
        for (SnapshotManifest.FileState file : table.files)
        {
            Map<Integer, Integer> rowGroups = new HashMap<>();
            for (SnapshotManifest.RowGroupState rowGroup : file.rowGroups)
            {
                rowGroups.put(rowGroup.rgId, rowGroup.recordNum);
            }
            topology.put(file.fileId, rowGroups);
        }
        return SnapshotIO.scanIndexSamples(directory.resolve(table.indexSamplesFile), sample ->
        {
            if (table.primaryIndex.canonicalKeyBytes > 0
                    && sample.key.length != table.primaryIndex.canonicalKeyBytes)
            {
                throw new IllegalArgumentException("index sample key length mismatch for "
                        + table.tableName);
            }
            if (sample.createTimestamp < 0
                    || sample.createTimestamp > manifest.snapshotTimestamp)
            {
                throw new IllegalArgumentException("index sample timestamp is outside [0,T_snap] for "
                        + table.tableName);
            }
            int bucket = IndexUtils.getBucketIdFromByteBuffer(ByteString.copyFrom(sample.key));
            if (sample.bucketId != bucket || bucket < 0 || bucket >= bucketCount)
            {
                throw new IllegalArgumentException("index sample bucket mismatch for "
                        + table.tableName);
            }
            Map<Integer, Integer> rowGroups = topology.get(sample.fileId);
            Integer recordNum = rowGroups == null ? null : rowGroups.get(sample.rgId);
            if (recordNum == null || sample.rgRowOffset < 0
                    || sample.rgRowOffset >= recordNum)
            {
                throw new IllegalArgumentException("index sample RowLocation is outside manifest "
                        + "topology for " + table.tableName);
            }
            return true;
        });
    }
}
