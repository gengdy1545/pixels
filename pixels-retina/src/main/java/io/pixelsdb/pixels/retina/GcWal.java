/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.retina;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

/**
 * Write-Ahead Log (WAL) for Storage GC rewrite operations.
 *
 * <p>Each GC rewrite records a {@code PENDING} entry <em>before</em> the new file
 * is registered in metadata. After the atomic metadata swap succeeds, a {@code DONE}
 * entry is appended. On process restart, any entry that has a {@code PENDING} line
 * but no matching {@code DONE} line represents an interrupted GC operation; the
 * caller is responsible for cleaning up the associated TEMPORARY metadata entry and
 * physical file.
 *
 * <h3>File format</h3>
 * One ASCII line per event, separated by {@code '\n'}:
 * <pre>
 *   PENDING {@literal <}newFileId{@literal >} {@literal <}newFilePath{@literal >}
 *   DONE    {@literal <}newFileId{@literal >}
 * </pre>
 *
 * <h3>Durability</h3>
 * Every {@link #beginEntry} and {@link #commitEntry} call flushes the underlying
 * {@link FileDescriptor#sync()} to guarantee the record survives a crash.
 *
 * <h3>Thread safety</h3>
 * All public methods are {@code synchronized} on {@code this}. Storage GC is
 * single-threaded, but the synchronization protects against any future path that
 * might invoke these methods concurrently.
 */
public class GcWal
{
    private static final Logger logger = LogManager.getLogger(GcWal.class);

    private static final String TOKEN_PENDING = "PENDING";
    private static final String TOKEN_DONE = "DONE";

    /** Absolute path of the WAL file on the local filesystem. */
    private final String walPath;

    public GcWal(String walPath)
    {
        this.walPath = walPath;
    }

    // -------------------------------------------------------------------------
    // Write side
    // -------------------------------------------------------------------------

    /**
     * Record the <em>start</em> of a GC rewrite operation.
     *
     * <p>Must be called (and durably flushed) <b>before</b> the new file is
     * registered in the metadata service as {@code TEMPORARY}.  This ensures that
     * even if the process crashes after registration but before the atomic swap,
     * the WAL contains a {@code PENDING} entry that the recovery path can use to
     * locate and remove the orphaned metadata/physical file.
     *
     * @param newFileId  the file ID assigned by the metadata service (pre-registered
     *                   as TEMPORARY)
     * @param newFilePath the full URI / path of the new physical file being written
     * @throws IOException if the WAL cannot be written or synced
     */
    public synchronized void beginEntry(long newFileId, String newFilePath) throws IOException
    {
        appendLine(TOKEN_PENDING + " " + newFileId + " " + newFilePath);
        logger.debug("GcWal: PENDING newFileId={}", newFileId);
    }

    /**
     * Record the <em>successful completion</em> of a GC rewrite operation.
     *
     * <p>Must be called <b>after</b> the atomic metadata swap
     * ({@code atomicSwapFiles}) has returned successfully.
     *
     * @param newFileId the file ID of the newly activated REGULAR file
     * @throws IOException if the WAL cannot be written or synced
     */
    public synchronized void commitEntry(long newFileId) throws IOException
    {
        appendLine(TOKEN_DONE + " " + newFileId);
        logger.debug("GcWal: DONE newFileId={}", newFileId);
    }

    // -------------------------------------------------------------------------
    // Recovery side
    // -------------------------------------------------------------------------

    /**
     * Parse the WAL and return all operations that started but did not complete.
     *
     * <p>An entry is considered pending when a {@code PENDING} line exists for a
     * given {@code newFileId} but no corresponding {@code DONE} line follows it.
     * Lines that cannot be parsed are silently skipped (with a warning log).
     *
     * @return list of {@link PendingEntry} objects for incomplete GC operations;
     *         empty if the WAL file does not exist or all operations completed
     * @throws IOException if the WAL file exists but cannot be read
     */
    public synchronized List<PendingEntry> recoverPendingEntries() throws IOException
    {
        File f = new File(walPath);
        if (!f.exists())
        {
            return Collections.emptyList();
        }

        // LinkedHashMap preserves insertion order so recovery is deterministic.
        Map<Long, String> pendingMap = new LinkedHashMap<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8)))
        {
            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null)
            {
                lineNo++;
                line = line.trim();
                if (line.isEmpty())
                {
                    continue;
                }

                if (line.startsWith(TOKEN_DONE + " "))
                {
                    String rest = line.substring(TOKEN_DONE.length() + 1).trim();
                    try
                    {
                        long fileId = Long.parseUnsignedLong(rest);
                        pendingMap.remove(fileId);
                    }
                    catch (NumberFormatException e)
                    {
                        logger.warn("GcWal: unparseable DONE line at line {}: '{}'", lineNo, line);
                    }
                }
                else if (line.startsWith(TOKEN_PENDING + " "))
                {
                    String rest = line.substring(TOKEN_PENDING.length() + 1).trim();
                    int spaceIdx = rest.indexOf(' ');
                    if (spaceIdx < 0)
                    {
                        logger.warn("GcWal: malformed PENDING line at line {}: '{}'", lineNo, line);
                        continue;
                    }
                    try
                    {
                        long fileId = Long.parseUnsignedLong(rest.substring(0, spaceIdx));
                        String filePath = rest.substring(spaceIdx + 1);
                        pendingMap.put(fileId, filePath);
                    }
                    catch (NumberFormatException e)
                    {
                        logger.warn("GcWal: unparseable PENDING line at line {}: '{}'", lineNo, line);
                    }
                }
                else
                {
                    logger.warn("GcWal: unrecognized line at line {}: '{}'", lineNo, line);
                }
            }
        }

        List<PendingEntry> result = new ArrayList<>(pendingMap.size());
        for (Map.Entry<Long, String> e : pendingMap.entrySet())
        {
            result.add(new PendingEntry(e.getKey(), e.getValue()));
        }
        if (!result.isEmpty())
        {
            logger.info("GcWal: found {} pending (incomplete) GC operations during recovery", result.size());
        }
        return result;
    }

    /**
     * Compact the WAL by rewriting it to contain only the supplied {@code remaining}
     * pending entries.
     *
     * <p>Typically called once after {@link #recoverPendingEntries()} has been
     * processed: if all pending operations were cleaned up successfully,
     * {@code remaining} is empty and the WAL file is effectively truncated.
     * If some pending operations could not be cleaned (e.g., metadata service
     * unavailable), pass them back so they are retried on the next restart.
     *
     * <p>The rewrite is performed atomically via a temp file + rename to avoid
     * a truncated WAL if the process crashes mid-compact.
     *
     * @param remaining pending entries that were NOT successfully cleaned up and
     *                  should be preserved for the next restart; may be empty
     * @throws IOException if the temp file cannot be written or the rename fails
     */
    public synchronized void compact(List<PendingEntry> remaining) throws IOException
    {
        File walFile = new File(walPath);
        File tmpFile = new File(walPath + ".tmp");

        try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(
                new FileOutputStream(tmpFile, false), StandardCharsets.UTF_8)))
        {
            for (PendingEntry entry : remaining)
            {
                writer.println(TOKEN_PENDING + " " + entry.newFileId + " " + entry.newFilePath);
            }
            writer.flush();
        }

        // Atomic rename (POSIX rename(2) is atomic on most filesystems).
        try
        {
            Files.move(tmpFile.toPath(), walFile.toPath(),
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        }
        catch (AtomicMoveNotSupportedException e)
        {
            // Fall back to non-atomic move on filesystems that don't support it
            // (rare on local disks, but possible on some network mounts).
            Files.move(tmpFile.toPath(), walFile.toPath(),
                    StandardCopyOption.REPLACE_EXISTING);
        }

        logger.info("GcWal: compacted WAL, {} pending entries retained", remaining.size());
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Append {@code line + '\n'} to the WAL file and call {@link FileDescriptor#sync()}
     * to guarantee the bytes reach durable storage before returning.
     */
    private void appendLine(String line) throws IOException
    {
        File f = new File(walPath);
        // Create parent directories on first write.
        File parent = f.getParentFile();
        if (parent != null && !parent.exists())
        {
            parent.mkdirs();
        }
        try (FileOutputStream fos = new FileOutputStream(f, /*append=*/true))
        {
            fos.write((line + "\n").getBytes(StandardCharsets.UTF_8));
            fos.flush();
            // fsync ensures the append is durable even if the OS page cache is not
            // flushed before the next crash.
            fos.getFD().sync();
        }
    }

    // -------------------------------------------------------------------------
    // Data types
    // -------------------------------------------------------------------------

    /**
     * An incomplete GC rewrite operation discovered during WAL recovery.
     */
    public static class PendingEntry
    {
        /** The file ID that was pre-registered as TEMPORARY in metadata. */
        public final long newFileId;
        /** The full physical path / URI of the new file being written. */
        public final String newFilePath;

        PendingEntry(long newFileId, String newFilePath)
        {
            this.newFileId = newFileId;
            this.newFilePath = newFilePath;
        }

        @Override
        public String toString()
        {
            return "PendingEntry{newFileId=" + newFileId + ", newFilePath='" + newFilePath + "'}";
        }
    }
}
