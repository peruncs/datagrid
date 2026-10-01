package peruncs.cluster.node.aeron;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.exceptions.AeronException;
import org.agrona.DirectBuffer;
import peruncs.cluster.errors.CorruptReplicationDataException;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.CRC32C;

/// Reconciles the short Archive tail after a writer restart using the Store mark.
///
/// The scan retains only two transaction summaries, never transaction payloads.
final class AeronWriterTailRecovery {
    private static final String REPLAY_CHANNEL = "aeron:ipc";

    private AeronWriterTailRecovery() {
    }

    /// Scans the bounded recording tail and returns the terminal markers needed to resume.
    static Result inspect(final Request request) {
        Objects.requireNonNull(request, "request");
        final long markSequence = request.mark().sequence();
        final long startPosition = markSequence >= 0L
                ? request.mark().prepareStartPosition() : request.recordingStartPosition();
        validateRecoveryBounds(markSequence, startPosition, request.recordingStartPosition(),
                request.stopPosition(), request.framing());
        final Scan scan = new Scan(request.mark(), request.clusterId(), request.epoch(), request.wireNonce(),
                request.maximumFencingToken(), request.framing().maxTransactionBytes());
        final long length = request.stopPosition() - startPosition;
        if (length > 0L) {
            scan.read(request.archive(), request.recordingId(), request.replayStreamId(),
                    request.replayTimeoutNanos(), request.stopPosition(), startPosition, length);
        }
        return scan.result(startPosition);
    }

    static long validateRecoveryBounds(final long markSequence, final long startPosition,
                                       final long recordingStartPosition, final long stopPosition,
                                       final Framing framing) {
        if (markSequence < -1L || startPosition < recordingStartPosition || stopPosition < startPosition) {
            throw reseed("writer mark is ahead of the Archive recording");
        }
        try {
            final long windowEnd = Math.addExact(startPosition, framing.recoveryWindowBytes());
            if (stopPosition > windowEnd) throw reseed("Archive tail exceeds writer recovery window");
            return windowEnd;
        } catch (final ArithmeticException overflow) {
            throw reseed("writer recovery window overflows Archive positions", overflow);
        }
    }

    /// Message sizes that determine how far a legitimate unfinished tail can extend in the Archive.
    ///
    /// Archive positions advance by framed lengths: every Aeron frame adds a data header and is
    /// aligned, a message larger than one MTU is split into several frames, and a message that does
    /// not fit the rest of a term leaves a padding frame behind.
    ///
    /// @param maxTransactionBytes largest dictionary-plus-data payload of one transaction
    /// @param chunkSize           largest payload of one replication message
    /// @param mtuLength           publication MTU
    /// @param termLength          Aeron term length
    record Framing(int maxTransactionBytes, int chunkSize, int mtuLength, int termLength) {
        private static final int FRAME_HEADER = 32;
        private static final int FRAME_ALIGNMENT_SLACK = 31;
        private static final int MARKER_FRAMES = 4;
        private static final int FRAMED_MARKER_BYTES = 128;

        Framing {
            if (maxTransactionBytes <= 0 || chunkSize <= 0 || chunkSize > maxTransactionBytes ||
                mtuLength <= FRAME_HEADER || termLength <= 0) {
                throw new IllegalArgumentException("invalid writer recovery bounds");
            }
        }

        /// Computes an upper bound of the Archive bytes spanned by two unfinished transactions.
        ///
        /// @return bound in Archive position units
        /// @throws ArithmeticException when the bound overflows a long
        long recoveryWindowBytes() {
            final long twoTransactions = Math.multiplyExact(2L, this.framedTransactionBytes());
            final long termRollovers = ceilDiv(twoTransactions, termLength) + 1L;
            return Math.addExact(Math.addExact(twoTransactions,
                            Math.multiplyExact(MARKER_FRAMES, FRAMED_MARKER_BYTES)),
                    Math.multiplyExact(termRollovers, this.largestFramedMessageBytes()));
        }

        /// Computes an upper bound of the Archive bytes one maximum-size transaction occupies.
        ///
        /// @return bound in Archive position units, including one possible term padding frame
        /// @throws ArithmeticException when the bound overflows a long
        long framedTransactionBytes() {
            final long maxPayload = mtuLength - FRAME_HEADER;
            final long perFrame = FRAME_HEADER + FRAME_ALIGNMENT_SLACK;
            /* Dictionary and data series each end with a partial chunk. */
            final long chunkMessages = ceilDiv(maxTransactionBytes, chunkSize) + 1L;
            final long payloadBytes = Math.addExact(maxTransactionBytes,
                    Math.multiplyExact(chunkMessages + 1L, AeronReplicationEnvelope.HEADER_LENGTH));
            final long frames = ceilDiv(payloadBytes, maxPayload) + chunkMessages + 2L;
            return Math.addExact(Math.addExact(payloadBytes, Math.multiplyExact(frames, perFrame)),
                    this.largestFramedMessageBytes());
        }

        private long largestFramedMessageBytes() {
            final long maxPayload = mtuLength - FRAME_HEADER;
            final long perFrame = FRAME_HEADER + FRAME_ALIGNMENT_SLACK;
            final long largestMessage = chunkSize + (long) AeronReplicationEnvelope.HEADER_LENGTH;
            return Math.addExact(largestMessage, Math.multiplyExact(ceilDiv(largestMessage, maxPayload), perFrame));
        }

        private static long ceilDiv(final long value, final long divisor) {
            return Math.floorDiv(value + divisor - 1L, divisor);
        }
    }

    private static ReseedRequiredException reseed(final String message) {
        return new ReseedRequiredException(message);
    }

    private static ReseedRequiredException reseed(final String message, final Throwable cause) {
        return new ReseedRequiredException(message, cause);
    }

    record Request(
            AeronArchive archive,
            ReplicationMark mark,
            UUID clusterId,
            long epoch,
            long wireNonce,
            long maximumFencingToken,
            long recordingId,
            long recordingStartPosition,
            long stopPosition,
            int replayStreamId,
            long replayTimeoutNanos,
            Framing framing
    ) {
        Request {
            Objects.requireNonNull(archive, "archive");
            Objects.requireNonNull(mark, "mark");
            Objects.requireNonNull(clusterId, "clusterId");
            Objects.requireNonNull(framing, "framing");
            if (recordingId < 0L || recordingStartPosition < 0L || stopPosition < 0L ||
                maximumFencingToken <= 0L || replayStreamId < 0 || replayTimeoutNanos <= 0L ||
                mark.sequence() == Long.MAX_VALUE || mark.sequence() >= 0L &&
                (mark.recordingId() != recordingId || mark.fencingToken() <= 0L || mark.prepareStartPosition() < 0L)) {
                throw new IllegalArgumentException("invalid writer recovery request");
            }
            if (mark.recordingId() >= 0L && mark.recordingId() != recordingId) {
                throw reseed("Store mark names a different Archive recording");
            }
        }
    }

    record Marker(long sequence, AeronReplicationEnvelope.Kind kind,
                  int payloadLength, int dataChunkCount, int dataCrc32c) {
    }

    record Result(long nextSequence, long boundarySequence, long boundaryPosition,
                  Marker markedCommit, Marker nextAbort) {
    }

    static final class Scan {
        private final UUID clusterId;
        private final long epoch;
        private final long wireNonce;
        private final long maximumFencingToken;
        private final int maxTransactionBytes;
        private final long markSequence;
        private final long nextSequence;
        private final AeronReplicationEnvelope.EnvelopeView envelope =
                new AeronReplicationEnvelope.EnvelopeView();
        private final Transaction marked;
        private final Transaction next;
        private final CRC32C dataCrc = new CRC32C();
        private ByteBuffer checksumSource;
        private ByteBuffer checksumView;
        private long lastPosition;
        private RuntimeException failure;

        Scan(final ReplicationMark mark, final UUID clusterId, final long epoch, final long wireNonce,
             final long maximumFencingToken, final int maxTransactionBytes) {
            this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
            Objects.requireNonNull(mark, "mark");
            if (epoch < 0L || wireNonce == 0L || maximumFencingToken <= 0L || maxTransactionBytes <= 0 ||
                mark.sequence() < -1L || mark.sequence() == Long.MAX_VALUE) {
                throw new IllegalArgumentException("invalid writer recovery scan");
            }
            this.epoch = epoch;
            this.wireNonce = wireNonce;
            this.maximumFencingToken = maximumFencingToken;
            this.maxTransactionBytes = maxTransactionBytes;
            this.markSequence = mark.sequence();
            this.nextSequence = Math.addExact(this.markSequence, 1L);
            this.marked = this.markSequence >= 0L ? new Transaction(this.markSequence) : null;
            this.next = new Transaction(this.nextSequence);
        }

        private void read(final AeronArchive archive, final long recordingId, final int replayStreamId,
                          final long replayTimeoutNanos, final long stopPosition,
                          final long startPosition, final long length) {
            this.lastPosition = startPosition;
            try (Subscription replay = archive.replay(
                    recordingId, startPosition, length, REPLAY_CHANNEL, replayStreamId)) {
                final FragmentAssembler fragments = new FragmentAssembler((buffer, offset, frameLength, header) -> {
                    try {
                        final long position = header.position();
                        if (position < startPosition || position > stopPosition) {
                            throw reseed("Archive replay returned a frame outside the inspected prefix");
                        }
                        final AeronReplicationEnvelope.EnvelopeView decoded =
                                AeronReplicationEnvelope.decodeView(buffer, offset, frameLength, this.envelope);
                        this.accept(buffer, decoded, position);
                        this.lastPosition = Math.max(this.lastPosition, position);
                    } catch (final CorruptReplicationDataException corrupt) {
                        this.failure = reseed("the Archive tail holds a corrupt frame", corrupt);
                    } catch (final RuntimeException invalid) {
                        this.failure = invalid;
                    }
                });
                final long deadline = System.nanoTime() + replayTimeoutNanos;
                while (this.lastPosition < stopPosition && this.failure == null &&
                       System.nanoTime() - deadline < 0L) {
                    if (Thread.currentThread().isInterrupted()) {
                        Thread.currentThread().interrupt();
                        throw new ReplicationUnavailableException("interrupted while replaying writer Archive tail");
                    }
                    if (replay.poll(fragments, 256) == 0) LockSupport.parkNanos(100_000L);
                }
                if (this.failure != null) throw this.failure;
                if (this.lastPosition < stopPosition) {
                    throw new ReplicationUnavailableException(
                            "Archive tail replay timed out before its stop position");
                }
            } catch (final ReseedRequiredException | ReplicationUnavailableException failure) {
                throw failure;
            } catch (final AeronException failure) {
                /* The Archive or its control channel did not answer: the recording and
                 * the Store mark are untouched, so a retry can still succeed. */
                throw new ReplicationUnavailableException("cannot replay writer Archive tail", failure);
            }
        }

        void accept(final DirectBuffer buffer,
                    final AeronReplicationEnvelope.EnvelopeView frame,
                    final long position) {
            if (!frame.matches(this.clusterId, this.wireNonce) || frame.epoch() != this.epoch ||
                frame.fencingToken() > this.maximumFencingToken) {
                throw reseed("Archive tail envelope identity does not match the Store mark");
            }
            final Transaction transaction;
            if (this.marked != null && frame.sequence() == this.markSequence) {
                transaction = this.marked;
            } else if (frame.sequence() == this.nextSequence) {
                transaction = this.next;
            } else {
                throw reseed("Archive tail sequence %d at position %d is outside the recovery window [%d, %d]"
                        .formatted(frame.sequence(), position, this.markSequence, this.nextSequence));
            }
            transaction.accept(buffer, frame, position, this);
        }

        private void updateDataCrc(final DirectBuffer buffer, final int offset, final int length) {
            if (length == 0) return;
            final byte[] array = buffer.byteArray();
            if (array != null) {
                this.dataCrc.update(array, buffer.wrapAdjustment() + offset, length);
                return;
            }
            final ByteBuffer backing = buffer.byteBuffer();
            if (backing == null) throw reseed("Archive replay buffer has no readable backing storage");
            if (backing != this.checksumSource) {
                this.checksumSource = backing;
                this.checksumView = backing.duplicate();
            }
            final int start = buffer.wrapAdjustment() + offset;
            this.checksumView.clear().position(start).limit(start + length);
            this.dataCrc.update(this.checksumView);
        }

        Result result(final long startPosition) {
            Marker markedCommit = null;
            Marker nextAbort = null;
            long boundarySequence = this.markSequence;
            long boundaryPosition = startPosition;
            if (this.marked != null) {
                if (!this.marked.completePrepare()) {
                    throw reseed("Store mark sequence has no complete Archive prepare");
                }
                if (this.marked.terminal == AeronReplicationEnvelope.Kind.ABORT) {
                    throw reseed("Store mark sequence is aborted in the Archive");
                }
                if (this.marked.terminal == null) {
                    markedCommit = this.marked.commitMarker();
                } else {
                    boundaryPosition = this.marked.terminalPosition;
                }
                if (this.next.seen && this.marked.terminal == null) {
                    throw reseed("Archive has a later sequence before the marked sequence is committed");
                }
            }
            if (this.next.seen) {
                if (this.next.terminal == AeronReplicationEnvelope.Kind.COMMIT) {
                    throw reseed("Archive contains a commit for a Store sequence not yet committed locally");
                }
                if (this.next.terminal == null) nextAbort = this.next.abortMarker();
                final long resolvedPosition = this.next.terminal == null ? -1L : this.next.terminalPosition;
                /* A missing marked commit cannot coexist with a later sequence: that case was
                 * rejected above, so the next sequence alone decides the boundary. */
                if (resolvedPosition >= 0L) boundaryPosition = resolvedPosition;
                boundarySequence = this.next.sequence;
                return new Result(Math.addExact(this.next.sequence, 1L), boundarySequence,
                        boundaryPosition, markedCommit, nextAbort);
            }
            if (markedCommit != null) boundaryPosition = -1L;
            return new Result(this.nextSequence, boundarySequence, boundaryPosition,
                    markedCommit, null);
        }
    }

    private static final class Transaction {
        private final long sequence;
        private boolean seen;
        private boolean dataStarted;
        private boolean terminalSeen;
        private int dictionaryLength = -1;
        private int dictionaryChunks;
        private int nextDictionaryChunk;
        private int dictionaryOffset;
        private int dataLength = -1;
        private int dataChunks;
        private int nextDataChunk;
        private int dataOffset;
        private int dataCrc32c;
        private AeronReplicationEnvelope.Kind terminal;
        private long terminalPosition = -1L;

        private Transaction(final long sequence) {
            this.sequence = sequence;
        }

        private void accept(final DirectBuffer buffer, final AeronReplicationEnvelope.EnvelopeView frame,
                            final long position, final Scan scan) {
            if (this.terminalSeen) throw reseed("Archive tail has data after a terminal marker");
            this.seen = true;
            switch (frame.kind()) {
                case TYPE_DICTIONARY -> this.acceptDictionary(frame, scan);
                case STORE_BINARY -> this.acceptData(buffer, frame, scan);
                case COMMIT -> this.acceptTerminal(frame, position, AeronReplicationEnvelope.Kind.COMMIT);
                case ABORT -> this.acceptTerminal(frame, position, AeronReplicationEnvelope.Kind.ABORT);
            }
        }

        private void acceptDictionary(final AeronReplicationEnvelope.EnvelopeView frame, final Scan scan) {
            if (this.dataStarted) throw reseed("type dictionary follows Store data in Archive tail");
            if (frame.payloadLength() > scan.maxTransactionBytes) {
                throw reseed("type dictionary exceeds the configured transaction bound");
            }
            if (this.dictionaryLength < 0) {
                if (frame.chunkIndex() != 0 || frame.chunkOffset() != 0) {
                    throw reseed("type dictionary does not start at its first chunk");
                }
                this.dictionaryLength = frame.payloadLength();
                this.dictionaryChunks = frame.chunkCount();
            }
            if (frame.payloadLength() != this.dictionaryLength || frame.chunkCount() != this.dictionaryChunks ||
                frame.chunkIndex() != this.nextDictionaryChunk || frame.chunkOffset() != this.dictionaryOffset) {
                throw reseed("type dictionary chunks are not contiguous");
            }
            this.dictionaryOffset += frame.payloadLengthOnWire();
            this.nextDictionaryChunk++;
        }

        private void acceptData(final DirectBuffer buffer, final AeronReplicationEnvelope.EnvelopeView frame,
                                final Scan scan) {
            if (this.dictionaryLength >= 0 && this.nextDictionaryChunk != this.dictionaryChunks) {
                throw reseed("Store data follows an incomplete type dictionary");
            }
            if (!this.dataStarted) {
                if (frame.chunkIndex() != 0 || frame.chunkOffset() != 0) {
                    throw reseed("Store data does not start at its first chunk");
                }
                if (frame.payloadLength() > scan.maxTransactionBytes - Math.max(0, this.dictionaryLength)) {
                    throw reseed("Store transaction exceeds the configured size bound");
                }
                this.dataStarted = true;
                this.dataLength = frame.payloadLength();
                this.dataChunks = frame.chunkCount();
                scan.dataCrc.reset();
            }
            if (frame.payloadLength() != this.dataLength || frame.chunkCount() != this.dataChunks ||
                frame.chunkIndex() != this.nextDataChunk || frame.chunkOffset() != this.dataOffset) {
                throw reseed("Store data chunks are not contiguous");
            }
            scan.updateDataCrc(buffer, frame.payloadOffset(), frame.payloadLengthOnWire());
            this.dataOffset += frame.payloadLengthOnWire();
            this.nextDataChunk++;
            this.dataCrc32c = (int) scan.dataCrc.getValue();
        }

        private void acceptTerminal(final AeronReplicationEnvelope.EnvelopeView frame, final long position,
                                    final AeronReplicationEnvelope.Kind kind) {
            if (kind == AeronReplicationEnvelope.Kind.COMMIT) {
                if (!this.completePrepare() || frame.payloadLength() != this.dataLength ||
                    frame.chunkCount() != this.dataChunks || frame.commitCrc32c() != this.dataCrc32c) {
                    throw reseed("Archive commit does not match a complete Store prepare");
                }
            } else if (this.dataStarted &&
                       (frame.payloadLength() != this.dataLength || frame.chunkCount() != this.dataChunks)) {
                throw reseed("Archive abort does not match its partial Store prepare");
            }
            this.terminal = kind;
            this.terminalPosition = position;
            this.terminalSeen = true;
        }

        private boolean completePrepare() {
            return this.dataStarted && this.dataLength >= 0 &&
                   (this.dictionaryLength < 0 || this.nextDictionaryChunk == this.dictionaryChunks &&
                    this.dictionaryOffset == this.dictionaryLength) &&
                   this.nextDataChunk == this.dataChunks && this.dataOffset == this.dataLength;
        }

        private Marker commitMarker() {
            return new Marker(this.sequence, AeronReplicationEnvelope.Kind.COMMIT,
                    this.dataLength, this.dataChunks, this.dataCrc32c);
        }

        private Marker abortMarker() {
            return new Marker(this.sequence, AeronReplicationEnvelope.Kind.ABORT,
                    Math.max(0, this.dataLength), Math.max(1, this.dataChunks), 0);
        }
    }
}
