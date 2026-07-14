/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.retina.benchmark.snapshot;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SnapshotManifestV2Test
{
    @TempDir
    Path snapshotDirectory;

    @Test
    void writesAndReadsMinimalV2Manifest() throws Exception
    {
        SnapshotManifest manifest = minimalManifest();

        SnapshotIO.writeManifest(snapshotDirectory, manifest);
        SnapshotManifest restored = SnapshotIO.readManifest(snapshotDirectory);

        assertEquals(SnapshotManifest.FORMAT_VERSION, restored.formatVersion);
        assertEquals("lineitem", restored.tables.get(0).tableName);
    }

    @Test
    void rejectsRuntimeEndpointInSemanticConfig()
    {
        SnapshotManifest manifest = minimalManifest();
        manifest.semanticConfig.put("etcd.hosts", "production-etcd");

        assertThrows(IllegalArgumentException.class,
                () -> SnapshotIO.writeManifest(snapshotDirectory, manifest));
    }

    @Test
    void derivesSnapshotTimestampFromEtcdHighWatermark()
    {
        assertEquals(41L, SnapshotExporter.snapshotTimestampFromHighWatermark("42"));
        assertThrows(IllegalStateException.class,
                () -> SnapshotExporter.snapshotTimestampFromHighWatermark("1"));
        assertThrows(IllegalStateException.class,
                () -> SnapshotExporter.snapshotTimestampFromHighWatermark("not-a-number"));
    }

    private static SnapshotManifest minimalManifest()
    {
        SnapshotManifest manifest = new SnapshotManifest();
        manifest.sourceHost = "source-node";
        manifest.snapshotTimestamp = 1L;
        manifest.semanticConfig.put("retina.tile.visibility.capacity", "10240");

        SnapshotManifest.TableState table = new SnapshotManifest.TableState();
        table.schemaName = "tpch";
        table.schemaId = 1L;
        table.tableName = "lineitem";
        table.tableId = 2L;
        table.tableType = "TABLE";
        table.storageScheme = "file";

        SnapshotManifest.ColumnState column = new SnapshotManifest.ColumnState();
        column.ordinal = 0;
        column.columnId = 3L;
        column.name = "orderkey";
        column.type = "bigint";
        table.columns.add(column);

        SnapshotManifest.LayoutState layout = new SnapshotManifest.LayoutState();
        layout.layoutId = 4L;
        layout.tableId = table.tableId;
        SnapshotManifest.PathState path = new SnapshotManifest.PathState();
        path.pathId = 5L;
        path.role = "ordered";
        path.uri = "file:///source/lineitem";
        layout.paths.add(path);
        table.layouts.add(layout);

        manifest.tables.add(table);
        return manifest;
    }
}
