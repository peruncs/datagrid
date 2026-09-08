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
import org.eclipse.serializer.afs.types.AWritableFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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

    @FunctionalInterface
    interface Creator
    {
        StoredMessageInfoManager create(AWritableFile offsetFile);
    }

    final class Default implements StoredMessageInfoManager
    {
        private static final Logger LOG = LoggerFactory.getLogger(StoredMessageInfoManager.class);

        private final AWritableFile messageInfoFile;
        private final MessageInfoParser messageInfoParser;

        private boolean closed = false;
        private boolean initialized = false;

        private MessageInfo messageInfo;

        private Default(final AWritableFile messageInfoFile, final MessageInfoParser messageInfoParser)
        {
            this.messageInfoFile = messageInfoFile;
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

            final var buffer = ByteBuffer.wrap(MessageInfoCodec.serializeBytes(messageInfo));

            try
            {
                this.messageInfoFile.truncate(0);
                // for some reason 0x0a was added to the end once, maybe some afs weirdness?
                final long written = this.messageInfoFile.writeBytes(buffer);
                if (LOG.isDebugEnabled() && messageInfo.messageIndex() % 10_000 == 0)
                {
                    LOG.debug("Stored message index {}, written {} bytes", messageInfo.messageIndex(), written);
                }
            }
            catch (final RuntimeException e)
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

            final boolean createdNew = this.messageInfoFile.ensureExists();

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
                    fileBytesBuffer = this.messageInfoFile.readBytes();
                }
                catch (final NodelibraryException e)
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

            try
            {
                this.messageInfoFile.release();
            }
            catch (final RuntimeException e)
            {
                LOG.error("Failed to release message info file", e);
            }

            this.closed = true;
        }
    }
}
