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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Versioned, portable description of the source state used by Retina benchmarks.
 *
 * <p>The manifest deliberately stores logical IDs and physical file/RG topology.
 * IndexKey embeds table/index IDs and RowLocation embeds file IDs, so assigning
 * fresh metadata IDs during restore would not reproduce the source state.</p>
 */
public final class SnapshotManifest
{
    public static final int FORMAT_VERSION = 1;
    public static final String FILE_NAME = "snapshot.json";

    public int formatVersion = FORMAT_VERSION;
    public String createdAtUtc;
    public String sourceHost;
    /** Retina vnodes assigned to the source host (provenance, not Index CF IDs). */
    public List<Integer> sourceVnodeIds = new ArrayList<>();
    /** RocksDB Index bucket/CF IDs physically present in this node snapshot. Empty means shared/default CF. */
    public List<Integer> sourceIndexBucketIds = new ArrayList<>();
    public boolean sourceQuiesced;
    public String consistencyNote;
    public long snapshotTimestamp;
    public boolean physicalIndexStateIncluded;
    public boolean visibilityCheckpointIncluded;
    public String visibilityCheckpointSource;
    public String visibilityCheckpointType;
    public String visibilityCheckpointHost;
    public long visibilityCheckpointTimestamp;
    public long visibilityCheckpointEntryCount;
    public long visibilityCheckpointExpectedEntryCount;
    public long visibilityCheckpointMatchedEntryCount;
    public long visibilityCheckpointUnmatchedEntryCount;
    public Map<String, String> effectiveConfig = new LinkedHashMap<>();
    public Map<String, String> artifactsSha256 = new LinkedHashMap<>();
    public List<TableState> tables = new ArrayList<>();

    public static final class TableState
    {
        public String schemaName;
        public long schemaId;
        public String tableName;
        public long tableId;
        public String tableType;
        public String storageScheme;
        public long metadataRowCount;
        public long footerRowsInSelectedSampleLayout;
        public long maxObservedCreateTimestamp;
        public List<ColumnState> columns = new ArrayList<>();
        public List<IndexState> indexes = new ArrayList<>();
        public IndexState primaryIndex;
        public List<LayoutState> layouts = new ArrayList<>();
        public List<FileState> files = new ArrayList<>();
        public String indexSamplesFile;
        public long indexSampleCount;
        public String rowSamplesFile;
        public long rowSampleCount;
        public String sampleLayout;
        public long selectedSampleLayoutId;
        /** Source layout/path selected by Retina when constructing a fresh WriteBuffer. */
        public long productionWriteLayoutId;
        public long productionOrderedPathId;
        public String productionOrderedPathUri;
        public long productionCompactPathId;
        public String productionCompactPathUri;
    }

    public static final class ColumnState
    {
        public int ordinal;
        public long columnId;
        public String name;
        public String type;
        public long cardinality;
        public double nullFraction;
    }

    public static final class IndexState
    {
        public long indexId;
        public String scheme;
        public boolean primary;
        public boolean unique;
        public long schemaVersionId;
        public List<Long> keyColumnIds = new ArrayList<>();
        public List<Integer> keyColumnOrdinals = new ArrayList<>();
        public List<String> keyColumnNames = new ArrayList<>();
        public List<String> keyColumnTypes = new ArrayList<>();
        /** Exact value derived by RocksDBFactory/IndexUtils; -1 means use configured default prefix. */
        public int rocksDbPrefixKeyBytes;
        public int canonicalKeyBytes;
    }

    public static final class LayoutState
    {
        public long layoutId;
        public long tableId;
        public long schemaVersionId;
        public long version;
        public long createAt;
        public String permission;
        public boolean readable;
        public boolean writable;
        public boolean productionSelectedLatestLayout;
        public List<PathState> paths = new ArrayList<>();
    }

    public static final class PathState
    {
        public int apiOrdinal;
        public long pathId;
        public String role;
        public String uri;
        public boolean productionSelectedByRetina;
    }

    public static final class FileState
    {
        public long fileId;
        public long pathId;
        public long layoutId;
        public String layoutRole;
        public String name;
        public String fullUri;
        public String type;
        public int metadataNumRowGroups;
        public long minRowId;
        public long maxRowId;
        public long fileSizeBytes;
        public long footerRowCount;
        public int footerRowGroupCount;
        public long pixelStride;
        public String compressionKind;
        public String fileVersion;
        public List<RowGroupState> rowGroups = new ArrayList<>();
    }

    public static final class RowGroupState
    {
        public int rgId;
        public int recordNum;
        public long dataLength;
        public long footerOffset;
        public long footerLength;
    }
}
