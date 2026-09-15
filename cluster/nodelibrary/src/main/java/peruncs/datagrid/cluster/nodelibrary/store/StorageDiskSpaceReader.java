package peruncs.datagrid.cluster.nodelibrary.store;


import org.eclipse.serializer.afs.types.ADirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

import static org.eclipse.serializer.util.X.notNull;

/**
 * This reader measures the bytes currently used by a storage directory.
 *
 * <p>A running Store may remove a file while the directory is being visited.
 * Implementations therefore treat that individual file as unavailable and
 * continue the measurement.</p>
 */
public interface StorageDiskSpaceReader
{
	/** Reads used bytes in the storage directory.
	 * @return used bytes
	 */
	long readUsedDiskSpaceBytes();

	/** Creates a disk-space reader.
	 * @param storageDir storage directory
	 * @return disk-space reader
	 */
	static StorageDiskSpaceReader New(final ADirectory storageDir)
	{
		return new Default(notNull(storageDir));
	}

	/** Recursively measures the configured Store directory. */
	class Default implements StorageDiskSpaceReader
	{
		private static final Logger LOG = LoggerFactory.getLogger(StorageDiskSpaceReader.class);
		private static final long CACHE_NANOS = 5_000_000_000L;
		private final ADirectory storageDir;

		private volatile long cachedBytes;
		private volatile long measuredAtNanos;
		private final AtomicLong lastLog = new AtomicLong(System.currentTimeMillis());

		private Default(final ADirectory storageDir)
		{
			this.storageDir = storageDir;
		}

		@Override
		public long readUsedDiskSpaceBytes()
		{
			final long now = System.nanoTime();
			final long measuredAt = this.measuredAtNanos;
			if (measuredAt != 0L && now - measuredAt >= 0L && now - measuredAt < CACHE_NANOS)
			{
				return this.cachedBytes;
			}
			final long sizeBytes;
			synchronized (this)
			{
				final long secondMeasuredAt = this.measuredAtNanos;
				final long secondNow = System.nanoTime();
				if (secondMeasuredAt != 0L && secondNow - secondMeasuredAt >= 0L &&
					secondNow - secondMeasuredAt < CACHE_NANOS)
				{
					return this.cachedBytes;
				}
				sizeBytes = this.totalSize(this.storageDir);
				this.cachedBytes = sizeBytes;
				this.measuredAtNanos = System.nanoTime();
			}
			final long nowMillis = System.currentTimeMillis();
			final long previousLog = this.lastLog.get();
			if (LOG.isTraceEnabled() && nowMillis - previousLog > 600_000L &&
				this.lastLog.compareAndSet(previousLog, nowMillis))
			{
				LOG.trace("Read current storage disk space ({})", sizeBytes);
			}
			return sizeBytes;
		}

		private long totalSize(final ADirectory dir)
		{
			final long[] total = {
				0L
			};
			dir.iterateFiles(f ->
			{
				try
				{
					try
					{
						total[0] = Math.addExact(total[0], f.size());
					}
					catch (final ArithmeticException overflow)
					{
						total[0] = Long.MAX_VALUE;
						LOG.warn("Storage size overflow while measuring {}", f);
					}
				}
				catch (final RuntimeException e)
				{
					LOG.debug("Could not measure storage file {}; it may have been removed", f, e);
				}
			});
			try
			{
				dir.iterateDirectories(d ->
				{
					final long child = this.totalSize(d);
					try { total[0] = Math.addExact(total[0], child); }
					catch (final ArithmeticException overflow) { total[0] = Long.MAX_VALUE; }
				});
			}
			catch (final RuntimeException failure)
			{
				LOG.debug("Could not measure a storage directory; it may have been removed", failure);
			}
			return total[0];
		}
	}
}
