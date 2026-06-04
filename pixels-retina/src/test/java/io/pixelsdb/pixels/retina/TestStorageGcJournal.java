/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the Affero
 * GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.retina;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.RollbackEntry;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for Storage GC journal persistence, store queries, and startup recovery.
 */
public class TestStorageGcJournal
{
    @Before
    public void setUp() throws IOException
    {
        cleanupJournalDir();
    }

    private static void cleanupJournalDir() throws IOException
    {
        String journalDir = ConfigFactory.Instance().getProperty("retina.storage.gc.journal.dir");
        Storage storage = StorageFactory.Instance().getStorage(journalDir);
        if (!storage.exists(journalDir))
        {
            return;
        }
        for (String path : storage.listPaths(journalDir))
        {
            storage.delete(path, false);
        }
    }

    private static StorageGcJournalStore newTestStore()
    {
        return new StorageGcJournalStore();
    }

    @Test
    public void testJournalTask_persistenceAndStateMachine()
    {
        StorageGcJournalTask task = StorageGcJournalTask.create(
                "task-1", 11L, 2, Arrays.asList(101L, 102L), 201L,
                "file:///tmp/pixels/gc-201.pxl", 3000L, 2);
        task = task.withRollbackEntry(new StorageGcJournalTask.RollbackEntry(key("pk-a"), 10L, 20L, true));
        task = task.transitionTo(StorageGcJournalTask.State.SWAPPED_NOT_CHECKPOINTED);

        StorageGcJournalTask decoded = StorageGcJournalTask.fromBytes(task.toBytes());
        assertEquals(task.getTaskId(), decoded.getTaskId());
        assertEquals(task.getNewFileId(), decoded.getNewFileId());
        assertEquals(StorageGcJournalTask.State.SWAPPED_NOT_CHECKPOINTED, decoded.getState());
        assertTrue(decoded.getRollbackEntries().get(0).isUpdated());

        StorageGcJournalTask checkpointed = decoded.transitionTo(StorageGcJournalTask.State.CHECKPOINTED);
        assertEquals(StorageGcJournalTask.State.CHECKPOINTED, checkpointed.getState());

        try
        {
            checkpointed.transitionTo(StorageGcJournalTask.State.ABORTED);
            fail("terminal CHECKPOINTED task must not transition to ABORTED");
        }
        catch (IllegalStateException expected)
        {
            assertTrue(expected.getMessage().contains("Invalid Storage GC journal transition"));
        }

        StorageGcJournalTask fresh = StorageGcJournalTask.create(
                "task-2", 11L, 2, Collections.singletonList(101L), 201L, 3000L, 2);
        try
        {
            fresh.transitionTo(StorageGcJournalTask.State.CHECKPOINTED);
            fail("INDEX_SWITCHING task must not skip SWAPPED_NOT_CHECKPOINTED");
        }
        catch (IllegalStateException expected)
        {
            assertTrue(expected.getMessage().contains("Invalid Storage GC journal transition"));
        }
    }

    @Test
    public void testJournalStore_gcWorkflowAndQueries() throws IOException
    {
        StorageGcJournalStore store = newTestStore();
        StorageGcJournalStore.Writer writer = new StorageGcJournalStore.Writer(store);
        store.createTask(StorageGcJournalTask.create(
                "task-1", 11L, 2, Arrays.asList(101L, 102L), 201L, 3000L, 2));

        IndexProto.IndexKey indexKey = key("pk-a");
        writer.recordBeforePrimarySwitch("task-1", indexKey, 10L, 20L);
        assertFalse(store.getTask("task-1").get().getRollbackEntries().get(0).isUpdated());
        writer.markPrimarySwitchUpdated("task-1", indexKey, 20L);
        store.transitionState("task-1", StorageGcJournalTask.State.SWAPPED_NOT_CHECKPOINTED);
        assertTrue(store.getTask("task-1").get().getRollbackEntries().get(0).isUpdated());

        Set<Long> queryFileIds = new HashSet<>(Arrays.asList(999L, 101L, 201L));
        assertTrue(store.collectPendingJournalFileIds().containsAll(Arrays.asList(101L, 102L, 201L)));
        Map<Long, List<StorageGcJournalTask>> pendingByFile = store.findPendingTasksByFileIds(queryFileIds);
        assertFalse(pendingByFile.containsKey(999L));
        assertEquals(1, pendingByFile.get(101L).size());
        assertEquals(1, pendingByFile.get(201L).size());

        store.createTask(StorageGcJournalTask.create(
                "task-terminal", 12L, 3, Collections.singletonList(102L), 202L, 4000L, 3)
                .transitionTo(StorageGcJournalTask.State.CHECKPOINTED));
        store.createTask(StorageGcJournalTask.create(
                "task-aborted", 13L, 4, Collections.singletonList(103L), 203L, 5000L, 4)
                .transitionTo(StorageGcJournalTask.State.ABORTED));

        List<StorageGcJournalTask> terminalTasks = store.listTerminalTasks();
        assertEquals(2, terminalTasks.size());
        store.deleteTerminalTasks(Arrays.asList("task-terminal", "task-aborted"));
        assertFalse(store.getTask("task-terminal").isPresent());
        assertFalse(store.getTask("task-aborted").isPresent());

        store.transitionState("task-1", StorageGcJournalTask.State.CHECKPOINTED);
        assertTrue(store.collectPendingJournalFileIds().isEmpty());
        assertTrue(store.findPendingTasksByFileIds(queryFileIds).isEmpty());

        store.createTask(StorageGcJournalTask.create(
                "task-pending", 14L, 5, Collections.singletonList(104L), 204L, 6000L, 5)
                .transitionTo(StorageGcJournalTask.State.SWAPPED_NOT_CHECKPOINTED));
        try
        {
            store.deleteTerminalTasks(Collections.singletonList("task-pending"));
            fail("deleteTerminalTasks must reject non-terminal tasks");
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }
    }

    @Test
    public void testJournalRecovery_acceptSwapAndMultiRoundLifecycle() throws Exception
    {
        StorageGcJournalStore store = newTestStore();
        store.createTask(StorageGcJournalTask.create(
                "first-round", 11L, 2, Collections.singletonList(101L), 201L, 3000L, 2)
                .transitionTo(StorageGcJournalTask.State.CHECKPOINTED));
        store.createTask(StorageGcJournalTask.create(
                "second-round", 11L, 3, Collections.singletonList(201L), 301L, 4000L, 3)
                .transitionTo(StorageGcJournalTask.State.SWAPPED_NOT_CHECKPOINTED));

        StorageGcJournalStore.RecoveryHandler handler = new StorageGcJournalStore.RecoveryHandler(
                store, mock(MetadataService.class), mock(IndexService.class));
        StorageGcJournalStore.RecoveryHandler.Result result =
                handler.recover(Collections.singleton(301L));

        assertEquals(1, result.getCheckpointed());
        assertEquals(0, result.getRolledBack());
        assertEquals(StorageGcJournalTask.State.CHECKPOINTED, store.getTask("second-round").get().getState());
        assertEquals(StorageGcJournalTask.State.CHECKPOINTED, store.getTask("first-round").get().getState());

        store.deleteTerminalTasks(Arrays.asList("first-round", "second-round"));
        assertFalse(store.getTask("first-round").isPresent());
        assertFalse(store.getTask("second-round").isPresent());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testJournalRecovery_rejectedSwapRollsBackCatalogAndIndex() throws Exception
    {
        StorageGcJournalStore store = newTestStore();
        IndexProto.IndexKey indexKey = key("pk-a");
        store.createTask(StorageGcJournalTask.create(
                "task-rejected", 11L, 2, Collections.singletonList(101L), 201L, 3000L, 2)
                .withRollbackEntry(new StorageGcJournalTask.RollbackEntry(indexKey, 10L, 20L, true))
                .transitionTo(StorageGcJournalTask.State.SWAPPED_NOT_CHECKPOINTED));

        MetadataService metadataService = mock(MetadataService.class);
        IndexService indexService = mock(IndexService.class);
        SinglePointIndex primaryIndex = new SinglePointIndex();
        primaryIndex.setId(22L);
        File oldFile = catalogFile(101L, File.Type.RETIRED, 123456L);
        File newFile = catalogFile(201L, File.Type.REGULAR, null);

        when(metadataService.getPrimaryIndex(11L)).thenReturn(primaryIndex);
        when(metadataService.getFileById(101L)).thenReturn(oldFile);
        when(metadataService.getFileById(201L)).thenReturn(newFile);
        when(metadataService.updateFile(any(File.class))).thenReturn(true);
        when(metadataService.deleteFiles(Collections.singletonList(201L))).thenReturn(true);

        StorageGcJournalStore.RecoveryHandler handler = new StorageGcJournalStore.RecoveryHandler(
                store, metadataService, indexService);
        StorageGcJournalStore.RecoveryHandler.Result result =
                handler.recover(Collections.singleton(101L));

        assertEquals(1, result.getRolledBack());
        assertEquals(1, result.getAborted());
        assertEquals(StorageGcJournalTask.State.ABORTED, store.getTask("task-rejected").get().getState());
        assertEquals(File.Type.REGULAR, oldFile.getType());
        assertEquals(null, oldFile.getCleanupAt());

        ArgumentCaptor<List> rollbackCaptor = ArgumentCaptor.forClass(List.class);
        verify(indexService).restorePrimaryIndexEntries(eq(11L), eq(22L), rollbackCaptor.capture(), any(IndexOption.class));
        RollbackEntry rollbackEntry = (RollbackEntry) rollbackCaptor.getValue().get(0);
        assertEquals(indexKey, rollbackEntry.getIndexKey());
        assertEquals(10L, rollbackEntry.getOldRowId());
        assertEquals(20L, rollbackEntry.getNewRowId());
        verify(indexService).deleteMainIndexRange(11L, 201L, 3000L, 2);
        verify(metadataService).updateFile(oldFile);
        verify(metadataService).deleteFiles(Collections.singletonList(201L));
    }

    @Test
    public void testJournalRecovery_incompleteAndAbortedTasks() throws Exception
    {
        StorageGcJournalStore store = newTestStore();
        IndexProto.IndexKey indexKey = key("pk-index-switching");
        store.createTask(StorageGcJournalTask.create(
                "task-index-switching", 11L, 2, Collections.singletonList(101L), 201L, 3000L, 2)
                .withRollbackEntry(new StorageGcJournalTask.RollbackEntry(indexKey, 10L, 20L, true)));
        store.createTask(StorageGcJournalTask.create(
                "task-aborted", 11L, 2, Collections.singletonList(105L), 205L, 5000L, 5)
                .transitionTo(StorageGcJournalTask.State.ABORTED));

        MetadataService metadataService = mock(MetadataService.class);
        IndexService indexService = mock(IndexService.class);
        SinglePointIndex primaryIndex = new SinglePointIndex();
        primaryIndex.setId(22L);
        File switchingOldFile = catalogFile(101L, File.Type.REGULAR, null);
        File switchingNewFile = catalogFile(201L, File.Type.TEMPORARY_GC, null);
        File abortedNewFile = catalogFile(205L, File.Type.TEMPORARY_GC, null);

        when(metadataService.getPrimaryIndex(11L)).thenReturn(primaryIndex);
        when(metadataService.getFileById(101L)).thenReturn(switchingOldFile);
        when(metadataService.getFileById(201L)).thenReturn(switchingNewFile);
        when(metadataService.getFileById(205L)).thenReturn(abortedNewFile);
        when(metadataService.deleteFiles(any())).thenReturn(true);

        StorageGcJournalStore.RecoveryHandler handler = new StorageGcJournalStore.RecoveryHandler(
                store, metadataService, indexService);
        StorageGcJournalStore.RecoveryHandler.Result result = handler.recover(Collections.emptySet());

        assertEquals(1, result.getRolledBack());
        assertEquals(1, result.getAborted());
        assertEquals(StorageGcJournalTask.State.ABORTED, store.getTask("task-index-switching").get().getState());
        assertEquals(StorageGcJournalTask.State.ABORTED, store.getTask("task-aborted").get().getState());
        verify(indexService).restorePrimaryIndexEntries(eq(11L), eq(22L),
                org.mockito.ArgumentMatchers.<List<RollbackEntry>>any(), any(IndexOption.class));
        verify(indexService).deleteMainIndexRange(11L, 201L, 3000L, 2);
        verify(indexService).deleteMainIndexRange(11L, 205L, 5000L, 5);
        verify(metadataService).deleteFiles(Collections.singletonList(201L));
        verify(metadataService).deleteFiles(Collections.singletonList(205L));
        verify(metadataService, never()).updateFile(switchingOldFile);
    }

    @Test
    public void testJournalRecovery_checkpointedMissingFromBaselineFailsClosed() throws Exception
    {
        StorageGcJournalStore store = newTestStore();
        store.createTask(StorageGcJournalTask.create(
                "task-final", 11L, 2, Collections.singletonList(101L), 201L, 3000L, 2)
                .transitionTo(StorageGcJournalTask.State.SWAPPED_NOT_CHECKPOINTED)
                .transitionTo(StorageGcJournalTask.State.CHECKPOINTED));

        StorageGcJournalStore.RecoveryHandler handler = new StorageGcJournalStore.RecoveryHandler(
                store, mock(MetadataService.class), mock(IndexService.class));
        try
        {
            handler.recover(Collections.singleton(101L));
            fail("CHECKPOINTED journal missing from baseline must fail closed");
        }
        catch (Exception e)
        {
            assertEquals(StorageGcJournalTask.State.CHECKPOINTED, store.getTask("task-final").get().getState());
        }
    }

    private static File catalogFile(long id, File.Type type, Long cleanupAt)
    {
        File file = new File();
        file.setId(id);
        file.setName("f-" + id + ".pxl");
        file.setType(type);
        file.setNumRowGroup(1);
        file.setMinRowId(0L);
        file.setMaxRowId(9L);
        file.setPathId(1L);
        file.setCleanupAt(cleanupAt);
        return file;
    }

    private static IndexProto.IndexKey key(String value)
    {
        return IndexProto.IndexKey.newBuilder()
                .setTableId(11L)
                .setIndexId(22L)
                .setKey(ByteString.copyFromUtf8(value))
                .setTimestamp(33L)
                .build();
    }
}
