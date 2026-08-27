package ru.myx.bdbje.util;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.ForwardCursor;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.ReadOptions;

/** @author myx */
public final class ForwardCursorFixedBuffer implements ForwardCursor {
	
	private static final int BUFFER_CAPACITY = 512;
	
	private static final int BUFFER_MASK = ForwardCursorFixedBuffer.BUFFER_CAPACITY - 1;
	
	private final Database database;
	
	private Record lastReturned = null;

	private byte[] nextKey = null;
	
	private Record[] queue = new Record[ForwardCursorFixedBuffer.BUFFER_CAPACITY];
	
	private int queueHead = 0;
	
	private int queueSize = 0;
	
	private int queueTail = 0;
	
	/** @param database */
	public ForwardCursorFixedBuffer(final Database database) {
		
		this.database = database;
	}

	/** @param database
	 * @param nextKey */
	public ForwardCursorFixedBuffer(final Database database, final byte[] nextKey) {
		
		this.database = database;
		if (nextKey != null) {
			this.nextKey = nextKey.clone();
		}
	}
	
	@Override
	public void close() throws DatabaseException {
		
		if (this.queue != null) {
			this.queue = null;
		}
		this.lastReturned = null;
	}
	
	@Override
	public OperationResult get(final DatabaseEntry key, final DatabaseEntry data, final Get get, final ReadOptions options) {
		
		switch (get) {
			case CURRENT : {
				final Record last = this.lastReturned;
				if (last == null) {
					throw new IllegalStateException("Current record is not set yet!");
				}
				key.setData(last.key);
				if (data != null) {
					data.setData(last.data);
				}
				return last.result;
			}
			case NEXT : {
				if (this.queue == null) {
					return null;
				}
				for (;;) {
					if (this.queueSize > 0) {
						final int index = --this.queueHead & ForwardCursorFixedBuffer.BUFFER_MASK;
						final Record next = this.lastReturned = this.queue[index];
						this.queue[index] = null;
						--this.queueSize;
						key.setData(next.key);
						if (data != null) {
							data.setData(next.data);
						}
						return next.result;
					}
					try (Cursor cursor = this.database.openCursor(null, CursorConfig.READ_COMMITTED);) {
						/** setup key */
						key.setData(
								this.nextKey == null
									? new byte[0]
									: this.nextKey//
						);
						/** last found record */
						Record record = null;
						OperationResult result = cursor.get(key, data, Get.SEARCH_GTE, null);
						buffer : for (;;) {
							if (result == null) {
								if (this.queueSize > 0) {
									break buffer;
								}
								this.queue = null;
								return null;
							}
							final int index = --this.queueTail & ForwardCursorFixedBuffer.BUFFER_MASK;
							record = this.queue[index] = new Record(key.getData(), data.getData(), result);
							if (++this.queueSize == ForwardCursorFixedBuffer.BUFFER_CAPACITY) {
								break buffer;
							}
							result = cursor.get(key, data, Get.NEXT, null);
						}
						if (record != null) {
							this.nextKey = StaticUtil.nextKey(record.key, false);
						}
					}
				}
			}
			default :
				throw new IllegalArgumentException("Invalid Get more for forward cursor: " + get);
		}
	}
	
	@Override
	public OperationStatus getCurrent(final DatabaseEntry key, final DatabaseEntry data, final LockMode lockMode) throws DatabaseException {
		
		final Record last = this.lastReturned;
		if (last == null) {
			return OperationStatus.KEYEMPTY;
		}
		key.setData(last.key);
		if (data != null) {
			data.setData(last.data);
		}
		return OperationStatus.SUCCESS;
	}
	
	@Override
	public Database getDatabase() {
		
		return this.database;
	}

	@Override
	public OperationStatus getNext(final DatabaseEntry key, final DatabaseEntry data, final LockMode lockMode) throws DatabaseException {
		
		if (this.queue == null) {
			return OperationStatus.NOTFOUND;
		}
		for (;;) {
			if (this.queueSize > 0) {
				final int index = --this.queueHead & ForwardCursorFixedBuffer.BUFFER_MASK;
				final Record next = this.lastReturned = this.queue[index];
				this.queue[index] = null;
				--this.queueSize;
				key.setData(next.key);
				if (data != null) {
					data.setData(next.data);
				}
				return OperationStatus.SUCCESS;
			}
			try (Cursor cursor = this.database.openCursor(null, CursorConfig.READ_COMMITTED);) {
				/** setup key */
				key.setData(
						this.nextKey == null
							? new byte[0]
							: this.nextKey//
				);
				/** last found record */
				Record record = null;
				buffer : for (OperationResult result = cursor.get(key, data, Get.SEARCH_GTE, null);;) {
					if (result == null) {
						if (this.queueSize > 0) {
							break buffer;
						}
						this.queue = null;
						return OperationStatus.NOTFOUND;
					}
					final int index = --this.queueTail & ForwardCursorFixedBuffer.BUFFER_MASK;
					record = this.queue[index] = new Record(key.getData(), data.getData(), result);
					if (++this.queueSize == ForwardCursorFixedBuffer.BUFFER_CAPACITY) {
						break buffer;
					}
					result = cursor.get(key, data, Get.NEXT, null);
				}
				if (record != null) {
					this.nextKey = StaticUtil.nextKey(record.key, false);
				}
			}
		}
	}
}
