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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.RollbackEntry;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;

/**
 * Durable store for Storage GC journal tasks. Each task is persisted as one
 * {@link RetinaUtils#STORAGE_GC_JOURNAL_SUFFIX} file under {@code journalDir}.
 */
public final class StorageGcJournalStore
{
    private static final int WRITE_BUFFER_SIZE = 4 * 1024 * 1024;
    private static final String JOURNAL_DIR_PROPERTY = "retina.storage.gc.journal.dir";

    private final Storage storage;
    private final String journalDir;

    public StorageGcJournalStore()
    {
        this.journalDir= ConfigFactory.Instance().getProperty(JOURNAL_DIR_PROPERTY);
        try
        {
            this.storage = StorageFactory.Instance().getStorage(journalDir);
            
        }
        catch (IOException e)
        {
            throw new IllegalStateException("Failed to initialize Storage GC journal store at " + dir, e);
        }
    }

    public synchronized void createTask(StorageGcJournalTask task)
    {
        Objects.requireNonNull(task, "task");
        if (containsTask(task.getTaskId()))
        {
            throw new IllegalArgumentException("Storage GC journal task already exists: " + task.getTaskId());
        }
        persistTask(task);
    }

    public synchronized Optional<StorageGcJournalTask> getTask(String taskId)
    {
        return loadTask(taskId);
    }

    public synchronized void appendRollbackEntry(String taskId, StorageGcJournalTask.RollbackEntry entry)
    {
        updateTask(taskId, task -> task.withRollbackEntry(entry));
    }

    public synchronized void markRollbackEntryUpdated(String taskId, IndexProto.IndexKey indexKey, long newRowId)
    {
        updateTask(taskId, task -> task.markRollbackEntryUpdated(indexKey, newRowId));
    }

    public synchronized void transitionState(String taskId, StorageGcJournalTask.State state)
    {
        updateTask(taskId, task -> task.transitionTo(state));
    }

    public synchronized List<StorageGcJournalTask> listTasksByState(StorageGcJournalTask.State state)
    {
        Objects.requireNonNull(state, "state");
        return filterTasks(loadAllTasks(), task -> task.getState() == state);
    }

    public synchronized Map<Long, List<StorageGcJournalTask>> findPendingTasksByFileIds(Collection<Long> fileIds)
    {
        return findPendingByFileIds(loadAllTasks(), fileIds);
    }

    /**
     * Delete terminal tasks (CHECKPOINTED or ABORTED) to prevent them from blocking future recoveries.
     */
    public synchronized void deleteTerminalTasks(Collection<String> taskIds)
    {
        if (taskIds == null || taskIds.isEmpty())
        {
            return;
        }
        for (String taskId : taskIds)
        {
            Optional<StorageGcJournalTask> task = loadTask(taskId);
            if (!task.isPresent())
            {
                continue;
            }
            if (!task.get().getState().isTerminal())
            {
                throw new IllegalArgumentException(
                        "Cannot delete non-terminal task " + taskId + " in state " + task.get().getState());
            }
            removeTask(taskId);
        }
    }

    /**
     * List all terminal tasks (CHECKPOINTED or ABORTED) that may need cleanup.
     */
    public synchronized List<StorageGcJournalTask> listTerminalTasks()
    {
        return filterTasks(loadAllTasks(), task -> task.getState().isTerminal());
    }

    public synchronized List<StorageGcJournalTask> listAllTasks()
    {
        return Collections.unmodifiableList(new ArrayList<>(loadAllTasks()));
    }

    public synchronized Set<Long> collectPendingJournalFileIds()
    {
        Set<Long> fileIds = new HashSet<>();
        for (StorageGcJournalTask task : loadAllTasks())
        {
            if (task.getState().isPending())
            {
                fileIds.add(task.getNewFileId());
                fileIds.addAll(task.getOldFileIds());
            }
        }
        return Collections.unmodifiableSet(fileIds);
    }

    private void updateTask(String taskId, Function<StorageGcJournalTask, StorageGcJournalTask> updater)
    {
        persistTask(updater.apply(requireTask(taskId)));
    }

    private StorageGcJournalTask requireTask(String taskId)
    {
        return loadTask(taskId).orElseThrow(
                () -> new IllegalArgumentException("Storage GC journal task not found: " + taskId));
    }

    private boolean containsTask(String taskId)
    {
        try
        {
            return storage.exists(taskPath(taskId));
        }
        catch (IOException e)
        {
            throw ioFailure("check Storage GC journal task " + taskId, e);
        }
    }

    private void persistTask(StorageGcJournalTask task)
    {
        try
        {
            try (DataOutputStream out = storage.create(taskPath(task.getTaskId()), true, WRITE_BUFFER_SIZE))
            {
                out.write(task.toBytes());
                out.flush();
            }
        }
        catch (IOException e)
        {
            throw ioFailure("persist Storage GC journal task " + task.getTaskId(), e);
        }
    }

    private Optional<StorageGcJournalTask> loadTask(String taskId)
    {
        String path = taskPath(taskId);
        try
        {
            if (!storage.exists(path))
            {
                return Optional.empty();
            }
            return Optional.of(readTask(path));
        }
        catch (IOException e)
        {
            throw ioFailure("read Storage GC journal task " + taskId, e);
        }
    }

    private List<StorageGcJournalTask> loadAllTasks()
    {
        try
        {
            if (!storage.exists(journalDir))
            {
                return new ArrayList<>();
            }
            List<StorageGcJournalTask> tasks = new ArrayList<>();
            for (String path : storage.listPaths(journalDir))
            {
                if (path != null && path.endsWith(RetinaUtils.STORAGE_GC_JOURNAL_SUFFIX))
                {
                    tasks.add(readTask(path));
                }
            }
            return tasks;
        }
        catch (IOException e)
        {
            throw ioFailure("list Storage GC journal tasks under " + journalDir, e);
        }
    }

    private void removeTask(String taskId)
    {
        String path = taskPath(taskId);
        try
        {
            if (storage.exists(path))
            {
                storage.delete(path, false);
            }
        }
        catch (IOException e)
        {
            throw ioFailure("delete Storage GC journal task " + taskId, e);
        }
    }

    private StorageGcJournalTask readTask(String path) throws IOException
    {
        long length = storage.getStatus(path).getLength();
        if (length <= 0 || length > Integer.MAX_VALUE)
        {
            throw new IOException("invalid Storage GC journal task size " + length + " at " + path);
        }
        byte[] bytes = new byte[(int) length];
        try (DataInputStream in = storage.open(path))
        {
            in.readFully(bytes);
        }
        return StorageGcJournalTask.fromBytes(bytes);
    }

    private String taskPath(String taskId)
    {
        return RetinaUtils.buildStorageGcJournalPath(journalDir, taskId);
    }

    private static List<StorageGcJournalTask> filterTasks(
            List<StorageGcJournalTask> tasks, Predicate<StorageGcJournalTask> predicate)
    {
        List<StorageGcJournalTask> result = new ArrayList<>();
        for (StorageGcJournalTask task : tasks)
        {
            if (predicate.test(task))
            {
                result.add(task);
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static Map<Long, List<StorageGcJournalTask>> findPendingByFileIds(
            List<StorageGcJournalTask> tasks, Collection<Long> fileIds)
    {
        if (fileIds == null || fileIds.isEmpty())
        {
            return Collections.emptyMap();
        }
        Set<Long> queryIds = new HashSet<>();
        for (Long fileId : fileIds)
        {
            if (fileId != null)
            {
                queryIds.add(fileId);
            }
        }
        if (queryIds.isEmpty())
        {
            return Collections.emptyMap();
        }

        Map<Long, List<StorageGcJournalTask>> result = new HashMap<>();
        for (StorageGcJournalTask task : tasks)
        {
            if (!task.getState().isPending())
            {
                continue;
            }
            for (Long fileId : queryIds)
            {
                if (task.isPendingForFile(fileId))
                {
                    result.computeIfAbsent(fileId, ignored -> new ArrayList<>()).add(task);
                }
            }
        }
        Map<Long, List<StorageGcJournalTask>> immutable = new HashMap<>(result.size());
        for (Map.Entry<Long, List<StorageGcJournalTask>> entry : result.entrySet())
        {
            immutable.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
        }
        return Collections.unmodifiableMap(immutable);
    }

    private static IllegalStateException ioFailure(String action, IOException cause)
    {
        return new IllegalStateException("Failed to " + action, cause);
    }

    // -------------------------------------------------------------------------
    // GC workflow helpers
    // -------------------------------------------------------------------------

    public static final class Writer
    {
        private final StorageGcJournalStore store;

        public Writer(StorageGcJournalStore store)
        {
            this.store = Objects.requireNonNull(store, "store");
        }

        public StorageGcJournalTask createTaskForRewrite(String taskId,
                                                           StorageGarbageCollector.RewriteResult result,
                                                           long tableId)
        {
            Objects.requireNonNull(result, "result");
            int totalRows = result.newFileRgRowStart[result.newFileRgCount];
            List<Long> oldFileIds = result.group.files.stream()
                    .map(fc -> fc.fileId)
                    .collect(Collectors.toList());
            StorageGcJournalTask task = StorageGcJournalTask.create(taskId, tableId,
                    result.group.virtualNodeId, oldFileIds, result.newFileId, result.newFilePath,
                    result.newRowIdStart, totalRows);
            store.createTask(task);
            return task;
        }

        public void recordBeforePrimarySwitch(String taskId,
                                              IndexProto.IndexKey indexKey,
                                              long oldRowId,
                                              long newRowId)
        {
            store.appendRollbackEntry(taskId,
                    new StorageGcJournalTask.RollbackEntry(indexKey, oldRowId, newRowId, false));
        }

        public void markPrimarySwitchUpdated(String taskId, IndexProto.IndexKey indexKey, long newRowId)
        {
            store.markRollbackEntryUpdated(taskId, indexKey, newRowId);
        }

        public void markSwappedNotCheckpointed(String taskId)
        {
            store.transitionState(taskId, StorageGcJournalTask.State.SWAPPED_NOT_CHECKPOINTED);
        }

        public void markAborted(String taskId)
        {
            store.transitionState(taskId, StorageGcJournalTask.State.ABORTED);
        }
    }

    public static final class RecoveryHandler
    {
        public static final class Result
        {
            private final int checkpointed;
            private final int aborted;
            private final int rolledBack;

            public Result(int checkpointed, int aborted, int rolledBack)
            {
                this.checkpointed = checkpointed;
                this.aborted = aborted;
                this.rolledBack = rolledBack;
            }

            public int getCheckpointed()
            {
                return checkpointed;
            }

            public int getAborted()
            {
                return aborted;
            }

            public int getRolledBack()
            {
                return rolledBack;
            }
        }

        private final StorageGcJournalStore journalStore;
        private final MetadataService metadataService;
        private final IndexService indexService;

        public RecoveryHandler(StorageGcJournalStore journalStore,
                               MetadataService metadataService,
                               IndexService indexService)
        {
            this.journalStore = Objects.requireNonNull(journalStore, "journalStore");
            this.metadataService = Objects.requireNonNull(metadataService, "metadataService");
            this.indexService = Objects.requireNonNull(indexService, "indexService");
        }

        public Result recover(Set<Long> baselineVisibleFileIds) throws RetinaException
        {
            Set<Long> baseline = baselineVisibleFileIds == null ? Collections.emptySet() : baselineVisibleFileIds;
            int checkpointed = 0;
            int aborted = 0;
            int rolledBack = 0;

            List<StorageGcJournalTask> swapped = new ArrayList<>();
            List<StorageGcJournalTask> indexSwitching = new ArrayList<>();
            List<StorageGcJournalTask> abortedTasks = new ArrayList<>();

            for (StorageGcJournalTask task : journalStore.listAllTasks())
            {
                switch (task.getState())
                {
                    case CHECKPOINTED:
                        if (!baseline.contains(task.getNewFileId()))
                        {
                            throw new RetinaException("Storage GC journal recovery failed: CHECKPOINTED task "
                                    + task.getTaskId() + " newFileId=" + task.getNewFileId()
                                    + " is absent from selected checkpoint baseline");
                        }
                        break;
                    case SWAPPED_NOT_CHECKPOINTED:
                        swapped.add(task);
                        break;
                    case INDEX_SWITCHING:
                        indexSwitching.add(task);
                        break;
                    case ABORTED:
                        abortedTasks.add(task);
                        break;
                    default:
                        break;
                }
            }

            for (StorageGcJournalTask task : swapped)
            {
                if (baseline.contains(task.getNewFileId()))
                {
                    journalStore.transitionState(task.getTaskId(), StorageGcJournalTask.State.CHECKPOINTED);
                    checkpointed++;
                }
                else
                {
                    rollbackTask(task, true);
                    rolledBack++;
                    aborted++;
                }
            }

            for (StorageGcJournalTask task : indexSwitching)
            {
                rollbackTask(task, isSwapCommitted(task));
                rolledBack++;
                aborted++;
            }

            for (StorageGcJournalTask task : abortedTasks)
            {
                cleanupNewFile(task);
            }

            return new Result(checkpointed, aborted, rolledBack);
        }

        private void rollbackTask(StorageGcJournalTask task, boolean restoreOldFiles) throws RetinaException
        {
            restorePrimaryIndex(task);
            if (restoreOldFiles)
            {
                restoreOldFileCatalog(task);
            }
            cleanupNewFile(task);
            journalStore.transitionState(task.getTaskId(), StorageGcJournalTask.State.ABORTED);
        }

        private void restorePrimaryIndex(StorageGcJournalTask task) throws RetinaException
        {
            List<RollbackEntry> entries = new ArrayList<>();
            for (StorageGcJournalTask.RollbackEntry entry : task.getRollbackEntries())
            {
                if (entry.getOldRowId() >= 0)
                {
                    entries.add(entry.toIndexRollbackEntry());
                }
            }
            if (entries.isEmpty())
            {
                return;
            }

            long primaryIndexId = getPrimaryIndexId(task.getTableId());
            IndexOption indexOption = IndexOption.builder().vNodeId(task.getVirtualNodeId()).build();
            try
            {
                indexService.restorePrimaryIndexEntries(task.getTableId(), primaryIndexId, entries, indexOption);
            }
            catch (IndexException | UnsupportedOperationException e)
            {
                throw new RetinaException("Storage GC journal recovery failed to restore primary index for taskId="
                        + task.getTaskId(), e);
            }
        }

        private long getPrimaryIndexId(long tableId) throws RetinaException
        {
            try
            {
                SinglePointIndex primaryIndex = metadataService.getPrimaryIndex(tableId);
                if (primaryIndex == null)
                {
                    throw new RetinaException("Storage GC journal recovery failed: primary index not found for tableId="
                            + tableId);
                }
                return primaryIndex.getId();
            }
            catch (MetadataException e)
            {
                throw new RetinaException("Storage GC journal recovery failed to load primary index for tableId="
                        + tableId, e);
            }
        }

        private void restoreOldFileCatalog(StorageGcJournalTask task) throws RetinaException
        {
            for (Long oldFileId : task.getOldFileIds())
            {
                File file = loadRequiredFile(task, oldFileId, "old file");
                if (file.getType() == File.Type.REGULAR)
                {
                    file.setCleanupAt(null);
                    continue;
                }
                if (file.getType() != File.Type.RETIRED)
                {
                    throw new RetinaException("Storage GC journal recovery failed: old fileId=" + oldFileId
                            + " for taskId=" + task.getTaskId() + " is " + file.getType()
                            + ", expected RETIRED or REGULAR");
                }
                file.setType(File.Type.REGULAR);
                file.setCleanupAt(null);
                updateFile(task, file, "restore old file catalog");
            }
        }

        private void cleanupNewFile(StorageGcJournalTask task) throws RetinaException
        {
            if (task.getNewRowCount() > 0)
            {
                try
                {
                    indexService.deleteMainIndexRange(task.getTableId(), task.getNewFileId(),
                            task.getNewRowIdStart(), task.getNewRowCount());
                }
                catch (IndexException | UnsupportedOperationException e)
                {
                    throw new RetinaException("Storage GC journal recovery failed to delete new MainIndex range for taskId="
                            + task.getTaskId(), e);
                }
            }

            File file = loadOptionalFile(task.getNewFileId());
            if (file != null)
            {
                if (file.getType() == File.Type.RETIRED)
                {
                    throw new RetinaException("Storage GC journal recovery failed: new fileId=" + task.getNewFileId()
                            + " for taskId=" + task.getTaskId() + " is RETIRED");
                }
                try
                {
                    if (!metadataService.deleteFiles(Collections.singletonList(task.getNewFileId())))
                    {
                        throw new RetinaException("deleteFiles returned false for newFileId=" + task.getNewFileId());
                    }
                }
                catch (MetadataException e)
                {
                    throw new RetinaException("Storage GC journal recovery failed to delete new file catalog for taskId="
                            + task.getTaskId(), e);
                }
            }

            String path = task.getNewFilePath();
            if (path != null && !path.trim().isEmpty())
            {
                try
                {
                    Storage storage = StorageFactory.Instance().getStorage(path);
                    if (storage.exists(path))
                    {
                        storage.delete(path, false);
                    }
                }
                catch (IOException e)
                {
                    throw new RetinaException("Storage GC journal recovery failed to delete new physical file for taskId="
                            + task.getTaskId() + ", path=" + path, e);
                }
            }
        }

        private boolean isSwapCommitted(StorageGcJournalTask task) throws RetinaException
        {
            File newFile = loadOptionalFile(task.getNewFileId());
            if (newFile != null && newFile.getType() == File.Type.REGULAR)
            {
                return true;
            }
            for (Long oldFileId : task.getOldFileIds())
            {
                File oldFile = loadOptionalFile(oldFileId);
                if (oldFile != null && oldFile.getType() == File.Type.RETIRED)
                {
                    return true;
                }
            }
            return false;
        }

        private File loadRequiredFile(StorageGcJournalTask task, long fileId, String role) throws RetinaException
        {
            File file = loadOptionalFile(fileId);
            if (file == null)
            {
                throw new RetinaException("Storage GC journal recovery failed: missing " + role
                        + " fileId=" + fileId + " for taskId=" + task.getTaskId());
            }
            return file;
        }

        private File loadOptionalFile(long fileId) throws RetinaException
        {
            try
            {
                return metadataService.getFileById(fileId);
            }
            catch (MetadataException e)
            {
                throw new RetinaException("Storage GC journal recovery failed to load fileId=" + fileId, e);
            }
        }

        private void updateFile(StorageGcJournalTask task, File file, String action) throws RetinaException
        {
            try
            {
                if (!metadataService.updateFile(file))
                {
                    throw new RetinaException(action + " returned false for fileId=" + file.getId());
                }
            }
            catch (MetadataException e)
            {
                throw new RetinaException("Storage GC journal recovery failed to " + action
                        + " for taskId=" + task.getTaskId(), e);
            }
        }
    }
}
