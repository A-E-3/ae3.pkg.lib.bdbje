/**
 * @author myx@myx.co.nz
 *
 * Free as a bird 8-)
 */
package ru.myx.bdbje.util;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import ru.myx.ae3.Engine;
import ru.myx.ae3.reflect.ReflectionExplicit;
import ru.myx.ae3.reflect.ReflectionManual;

/** Cleans 'deleted' folder when BDB JE is running in 'delete-expunge' mode
 *
 * @author myx */
@ReflectionManual
public class DelayExpungeTask implements Runnable, FileFilter, DirectoryStream.Filter<Path> {
	
	private final File folder;
	
	private final String filenameExt;
	
	private long currentDeadline;
	
	/** Last iteration date */
	@ReflectionExplicit
	public long lastDate = -1;
	
	/** Last deleted file TTL */
	@ReflectionExplicit
	public long lastTTL = -1;
	
	/** Last available space */
	@ReflectionExplicit
	public double lastAvail = -1;
	
	/** Number of files staying in the queue after last check */
	@ReflectionExplicit
	private int lastQueued = -1;
	
	/** Number of files deleted from disk during last check */
	@ReflectionExplicit
	private int lastCleaned = -1;
	
	/** Number of files or directories ignored during last check */
	@ReflectionExplicit
	private int lastIgnored = -1;
	
	/** @param folder */
	public DelayExpungeTask(final File folder) {
		
		this(folder, ".del");
	}
	
	/** @param folder
	 * @param filenameExt */
	public DelayExpungeTask(final File folder, final String filenameExt) {
		
		this.folder = folder;
		this.filenameExt = filenameExt;
		
		if (folder.exists()) {
			if (!folder.isDirectory()) {
				throw new IllegalArgumentException("The folder must be a directory: " + folder.getAbsolutePath());
			}
		} else {
			if (!folder.mkdirs()) {
				throw new IllegalArgumentException("Can't create the directory: " + folder.getAbsolutePath());
			}
		}
		this.calculate();
	}
	
	@Override
	public boolean accept(final File file) {
		
		if (!file.isFile() || !file.getName().endsWith(this.filenameExt)) {
			return false;
		}
		if (file.lastModified() > this.currentDeadline) {
			return false;
		}
		// file.delete();
		// return false;
		return true;
	}
	
	@Override
	public boolean accept(final Path path) {
		
		final File file = path.toFile();
		if (!file.isFile() || !file.getName().endsWith(this.filenameExt)) {
			return false;
		}
		if (file.lastModified() > this.currentDeadline) {
			return false;
		}
		// file.delete();
		// return false;
		return true;
	}
	
	private void calculate() {
		
		final long diskFree = Math.min(this.folder.getUsableSpace(), this.folder.getFreeSpace());
		final long diskSize = this.folder.getTotalSpace();
		
		final double avail = Math.min(1.0, Math.max(0.0, 1.0 * (diskFree - this.getBytesReserveMinimum()) / diskSize));
		
		final long keepMin = 15 * 1000L;
		final long keepMax = 17 * 60 * 60 * 1000L;
		
		final long ttl = (long) (keepMin * (1.0 - avail) + keepMax * avail);
		
		this.lastTTL = ttl;
		this.lastAvail = avail;
	}
	
	/** @return */
	public long getBytesAllowedAvailable() {
		
		final long diskFree = Math.min(this.folder.getUsableSpace(), this.folder.getFreeSpace());
		return Math.max(0, diskFree - this.getBytesReserveMinimum());
	}
	
	/** 17% or 7.0G is reserved
	 *
	 * @return reserved space we aren't allowed to use in bytes */
	@ReflectionExplicit
	public long getBytesReserveMinimum() {
		
		final long diskSize = this.folder.getTotalSpace();
		return Math.max((long) (diskSize * 17.0 / 100.0), 7 * 1024 * 1024 * 1024L);
	}
	
	/** @return */
	@ReflectionExplicit
	public String getQueueLocation() {
		
		return Path.of(this.folder.getAbsolutePath(), "*" + this.filenameExt).toAbsolutePath().toString();
	}
	
	/** prefixed ('disk' and 'keep') locals are for readability only. */
	@Override
	public void run() {
		
		this.calculate();
		this.lastDate = Engine.fastTime();
		this.currentDeadline = this.lastDate - this.lastTTL;
		
		int other = 0, queue = 0, clean = 0;
		
		// this.folder.listFiles(this);
		// Files.newDirectoryStream(this.folder.toPath(), this)
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(this.folder.toPath())) {
			for (final Path path : stream) {
				final File file = path.toFile();
				if (!file.isFile() || !file.getName().endsWith(this.filenameExt)) {
					++other;
					continue;
				}
				if (file.lastModified() > this.currentDeadline) {
					++queue;
					continue;
				}
				file.delete();
				++clean;
				continue;
			}
		} catch (final IOException ex) {
			// An I/O problem has occurred
		}
		
		this.lastQueued = queue;
		this.lastCleaned = clean;
		this.lastIgnored = other;
	}
}
