package peruncs.datagrid.cluster.nodelibrary.replication;

import peruncs.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static org.eclipse.serializer.util.X.notNull;

/**
 * This manager persists the last replication cursor accepted by a node.
 *
 * <p>The stored position is the restart boundary. A successful write means a
 * later reader may resume from that position; a failed write leaves the
 * boundary uncertain and must be treated as a startup error.</p>
 */
public interface StoredReplicationCursorManager extends AutoCloseable
{
	    /** Returns the last stored replication cursor.
	     * @return stored replication cursor
	     * @throws NodelibraryException if reading fails
	     */
    ReplicationCursor get() throws NodelibraryException;

	    /** Stores a replication cursor as the restart boundary.
	     * @param cursor replication cursor
	     * @throws NodelibraryException if writing fails
	     */
    void set(ReplicationCursor cursor) throws NodelibraryException;

    @Override
    void close();

	/** Creates a forced binary cursor manager for a native path.
     *
	 * <p>Cursor reads and writes use the shared atomic cursor-file protocol. If a
	 * write fails, the caller must retain uncertainty and fail closed rather than
	 * treating the import as checkpointed.</p>
     *
     * @param cursorPath metadata path
     * @return atomic replication-cursor manager
     */
    static StoredReplicationCursorManager NewAtomic(final Path cursorPath)
    {
        return new Default(notNull(cursorPath));
    }

	/** Persists replication cursors with one forced binary backend. */
	final class Default implements StoredReplicationCursorManager
    {
        private static final Logger LOG = LoggerFactory.getLogger(StoredReplicationCursorManager.class);

        private final Path path;

        private boolean closed = false;
        private boolean initialized = false;

        private ReplicationCursor cursor;

        private Default(final Path path)
        {
            this.path = path;
        }

        @Override
        public synchronized ReplicationCursor get() throws NodelibraryException
        {
            this.ensureOpen();
            this.ensureInit();
            return this.cursor;
        }

        @Override
        public synchronized void set(final ReplicationCursor cursor) throws NodelibraryException
        {
            this.ensureOpen();
            this.ensureInit();

            try
            {
                ReplicationCursorStore.write(this.path, cursor);
                final long written = cursor.providerPosition().length;
                if (LOG.isDebugEnabled() && cursor.logicalSequence() % 10_000 == 0)
                {
                    LOG.debug("Stored replication sequence {}, written {} bytes", cursor.logicalSequence(), written);
                }
            }
            catch (final IOException | RuntimeException e)
            {
                throw new NodelibraryException("Failed to write replication cursor file", e);
            }

            this.cursor = cursor;
        }

        private void ensureInit() throws NodelibraryException
        {
            this.ensureOpen();
            if (this.initialized)
            {
                return;
            }

            LOG.info("Initializing StoredReplicationCursorManager");

            try
            {
                LOG.debug("Reading existing replication cursor file.");
				this.cursor = ReplicationCursorStore.read(this.path);
                LOG.debug("Read previous replication sequence at {}", this.cursor.logicalSequence());
            }
            catch (final NoSuchFileException missing)
            {
                LOG.debug("New replication cursor file has been created.");
                this.cursor = new ReplicationCursor("none", null, -1, new byte[0]);
            }
            catch (final IOException failure)
            {
                throw new NodelibraryException("Failed to read binary replication cursor " + this.path, failure);
            }

            this.initialized = true;
        }

        private void ensureOpen()
        {
            if (this.closed)
            {
                throw new IllegalStateException("StoredReplicationCursorManager is closed");
            }
        }

        @Override
        public synchronized void close()
        {
            if (this.closed)
            {
                return;
            }
            LOG.trace("Closing StoredReplicationCursorManager");
            this.closed = true;
        }
    }
}
