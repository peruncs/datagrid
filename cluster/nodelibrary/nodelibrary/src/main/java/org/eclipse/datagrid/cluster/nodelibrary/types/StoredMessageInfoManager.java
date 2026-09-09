package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.storage.distributed.types.AtomicFileStore;
import org.eclipse.serializer.afs.types.AWritableFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.eclipse.serializer.util.X.notNull;

public interface StoredMessageInfoManager extends AutoCloseable
{
    MessageInfo get() throws NodelibraryException;

    void set(MessageInfo messageInfoInfo) throws NodelibraryException;

    @Override
    void close();

    static StoredMessageInfoManager New(final AWritableFile messageInfoFile, final MessageInfoParser messageInfoParser)
    {
        return new Default(notNull(messageInfoFile), notNull(messageInfoParser));
    }

    /** Creates a forced temporary-file replacement manager for a native path. */
    static StoredMessageInfoManager NewAtomic(final Path messageInfoPath, final MessageInfoParser messageInfoParser)
    {
        return new Default(notNull(messageInfoPath), notNull(messageInfoParser));
    }

    @FunctionalInterface
    interface Creator
    {
        StoredMessageInfoManager create(AWritableFile offsetFile);
    }

    final class Default implements StoredMessageInfoManager
    {
        private static final Logger LOG = LoggerFactory.getLogger(StoredMessageInfoManager.class);

        private final AWritableFile messageInfoFile;
        private final Path atomicPath;
        private final MessageInfoParser messageInfoParser;

        private boolean closed = false;
        private boolean initialized = false;

        private MessageInfo messageInfo;

        private Default(final AWritableFile messageInfoFile, final MessageInfoParser messageInfoParser)
        {
            this.messageInfoFile = messageInfoFile;
            this.atomicPath = null;
            this.messageInfoParser = messageInfoParser;
        }

        private Default(final Path atomicPath, final MessageInfoParser messageInfoParser)
        {
            this.messageInfoFile = null;
            this.atomicPath = atomicPath;
            this.messageInfoParser = messageInfoParser;
        }

        @Override
        public synchronized MessageInfo get() throws NodelibraryException
        {
            this.ensureInit();
            return this.messageInfo;
        }

        @Override
        public synchronized void set(final MessageInfo messageInfo) throws NodelibraryException
        {
            this.ensureInit();

            try
            {
                final byte[] serialized = MessageInfoCodec.serializeBytes(messageInfo);
                final long written;
                if (this.atomicPath != null)
                {
					AtomicFileStore.write(this.atomicPath, channel ->
					{
						final ByteBuffer buffer = ByteBuffer.wrap(serialized);
						while (buffer.hasRemaining()) channel.write(buffer);
					});
                    written = serialized.length;
                }
                else
                {
                    final var buffer = ByteBuffer.wrap(serialized);
                    this.messageInfoFile.truncate(0);
                    // for some reason 0x0a was added to the end once, maybe some afs weirdness?
                    written = this.messageInfoFile.writeBytes(buffer);
                }
                if (LOG.isDebugEnabled() && messageInfo.messageIndex() % 10_000 == 0)
                {
                    LOG.debug("Stored message index {}, written {} bytes", messageInfo.messageIndex(), written);
                }
            }
            catch (final IOException | RuntimeException e)
            {
                throw new NodelibraryException("Failed to write message info file", e);
            }

            this.messageInfo = messageInfo;
        }

        private void ensureInit() throws NodelibraryException
        {
            if (this.initialized)
            {
                return;
            }

            LOG.info("Initializing StoredMessageInfoManager");

            final boolean createdNew;
            if (this.atomicPath != null)
            {
                try
                {
                    final Path absolute = this.atomicPath.toAbsolutePath();
                    final Path parent = absolute.getParent();
                    if (parent != null) Files.createDirectories(parent);
                    createdNew = Files.notExists(absolute);
                }
                catch (final IOException failure)
                {
                    throw new NodelibraryException("Failed to prepare message info file", failure);
                }
            }
            else
            {
                createdNew = this.messageInfoFile.ensureExists();
            }

            if (createdNew)
            {
                LOG.debug("New message info file has been created.");
                this.messageInfo = MessageInfo.New(Long.MIN_VALUE);
            }
            else
            {
                LOG.debug("Reading existing message info file.");
                final ByteBuffer fileBytesBuffer;
                try
                {
                    fileBytesBuffer = this.atomicPath == null
                        ? this.messageInfoFile.readBytes()
                        : ByteBuffer.wrap(Files.readAllBytes(this.atomicPath));
                }
                catch (final IOException | NodelibraryException e)
                {
                    throw new NodelibraryException("Failed to read message info file", e);
                }

                if (fileBytesBuffer.remaining() == 0)
                {
                    LOG.debug("Previous message info file is empty");
                    this.messageInfo = MessageInfo.New(Long.MIN_VALUE);
                }
                else
                {
                    this.messageInfo = this.parseMessageInfo(fileBytesBuffer);
                    LOG.debug("Read previous message index at {}", this.messageInfo.messageIndex());
                }
            }

            this.initialized = true;
        }

        private MessageInfo parseMessageInfo(final ByteBuffer buffer) throws NodelibraryException
        {
            final var bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return this.messageInfoParser.parseMessageInfo(new String(bytes, StandardCharsets.UTF_8));
        }

        @Override
        public synchronized void close()
        {
            if (this.closed)
            {
                return;

            }
            LOG.trace("Closing StoredMessageInfoManager");

            if (this.messageInfoFile != null)
            {
                try
                {
                    this.messageInfoFile.release();
                }
                catch (final RuntimeException e)
                {
                    LOG.error("Failed to release message info file", e);
                }
            }

            this.closed = true;
        }
    }
}
