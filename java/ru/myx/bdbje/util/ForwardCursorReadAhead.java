package ru.myx.bdbje.util;

import java.util.concurrent.TimeUnit;

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

import ru.myx.util.FifoQueueLinked;

/** @author myx */
public final class ForwardCursorReadAhead implements ForwardCursor, Runnable {
	
	private static final int BATCH_LIMIT_ROWS = 32768;
	
	private static final long BATCH_LIMIT_TIME_NS = TimeUnit.SECONDS.toNanos(60);
	
	private static final int BUFFER_LIMIT_ROWS = 512;
	
	private static final long BUFFER_LIMIT_BYTES = 2 * 1024 * 1024;
	
	private FifoQueueLinked<Record> queue = new FifoQueueLinked<>();
	
	private FifoQueueLinked<Record> queueAhead = null;
	
	private Record lastReturned = null;
	
	private byte[] nextKey = null;
	
	private final Database database;
	
	private final DatabaseEntry key = new DatabaseEntry();
	
	private final DatabaseEntry data = new DatabaseEntry();
	
	private boolean finished = false;
	
	/** @param database */
	public ForwardCursorReadAhead(final Database database) {
		
		this.database = database;
		new Thread(this).start();
	}
	
	/** @param database
	 * @param nextKey */
	public ForwardCursorReadAhead(final Database database, final byte[] nextKey) {
		
		this.database = database;
		if (nextKey != null) {
			this.nextKey = nextKey.clone();
		}
		new Thread(this).start();
	}
	
	@Override
	public void close() throws DatabaseException {
		
		if (!this.finished) {
			synchronized (this) {
				if (!this.finished) {
					this.finished = true;
					this.notifyAll();
				}
			}
		}
		if (this.queue != null) {
			this.queue = null;
		}
		if (this.queueAhead != null) {
			this.queueAhead = null;
		}
		this.lastReturned = null;
		this.nextKey = null;
	}
	
	@Override
	public OperationResult get(final DatabaseEntry key, final DatabaseEntry data, final Get get, final ReadOptions options) throws DatabaseException {
		
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
				poll : for (;;) {
					{
						final Record next = this.queue.pollFirst();
						if (next != null) {
							this.lastReturned = next;
							key.setData(next.key);
							if (data != null) {
								data.setData(next.data);
							}
							return next.result;
						}
					}
					wait : for (;;) {
						synchronized (this) {
							if (this.queueAhead != null) {
								this.queue = this.queueAhead;
								this.queueAhead = null;
								this.notifyAll();
								continue poll;
							}
							if (this.finished) {
								return null;
							}
							try {
								this.wait(555L);
								continue wait;
							} catch (final InterruptedException e) {
								this.finished = true;
								return null;
							}
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
		
		poll : for (;;) {
			{
				final Record next = this.queue.pollFirst();
				if (next != null) {
					this.lastReturned = next;
					key.setData(next.key);
					if (data != null) {
						data.setData(next.data);
					}
					return OperationStatus.SUCCESS;
				}
			}
			wait : for (;;) {
				synchronized (this) {
					if (this.queueAhead != null) {
						this.queue = this.queueAhead;
						this.queueAhead = null;
						this.notifyAll();
						continue poll;
					}
					if (this.finished) {
						return OperationStatus.NOTFOUND;
					}
					try {
						this.wait(555L);
						continue wait;
					} catch (final InterruptedException e) {
						this.finished = true;
						return OperationStatus.NOTFOUND;
					}
				}
			}
		}
	}
	
	@Override
	public void run() {
		
		/** ramaining records capacity in current buffer */
		int bufferRowsRemain = 0;
		/** ramaining byte size capacity in current buffer */
		long bufferBytesRemain = 0;
		
		main : for (;;) {
			if (this.finished) {
				return;
			}
			try (Cursor cursor = this.database.openCursor(null, CursorConfig.READ_COMMITTED);) {
				/** setup key */
				this.key.setData(
						this.nextKey == null
							? new byte[0]
							: this.nextKey//
				);
				/** max records on one cursor */
				int batchRowsRemain = ForwardCursorReadAhead.BATCH_LIMIT_ROWS;
				final long batchExpiresAt = System.nanoTime() + ForwardCursorReadAhead.BATCH_LIMIT_TIME_NS;
				/** last found record */
				Record record = null;
				/** seek */
				OperationResult result = cursor.get(this.key, this.data, Get.SEARCH_GTE, null);
				buffer : for (;;) {
					/** no more records */
					if (result == null) {
						synchronized (this) {
							if (!this.finished) {
								this.finished = true;
								this.notifyAll();
							}
							return;
						}
					}
					record = new Record(this.key.getData(), this.data.getData(), result);
					synchronized (this) {
						if (this.queueAhead == null) {
							this.queueAhead = new FifoQueueLinked<>();
							bufferRowsRemain = ForwardCursorReadAhead.BUFFER_LIMIT_ROWS;
							bufferBytesRemain = ForwardCursorReadAhead.BUFFER_LIMIT_BYTES;
							this.notifyAll();
						}
						this.queueAhead.offerLast(record);
						if (--batchRowsRemain <= 0 || batchExpiresAt < System.nanoTime()) {
							this.nextKey = StaticUtil.nextKey(record.key, false);
							break buffer;
						}
						if (--bufferRowsRemain == 0 || (bufferBytesRemain -= record.key.length + (record.data == null
							? 0
							: record.data.length)) < 0) {
							/** make early, so it's not fucked up by client yet */
							this.nextKey = StaticUtil.nextKey(record.key, false);
							try {
								this.wait(200L);
							} catch (final InterruptedException e) {
								this.finished = true;
								return;
							}
							if (this.queueAhead != null) {
								/** stop batch, queue is not consumed within time allotted */
								break buffer;
							}
							if (this.finished) {
								return;
							}
							/** fall-trough, continue buffer */
						}
					}
					/** next record */
					result = cursor.get(this.key, this.data, Get.NEXT, null);
				}
			}
			for (;;) {
				synchronized (this) {
					if (this.queueAhead == null) {
						continue main;
					}
					if (this.finished) {
						return;
					}
					try {
						this.wait(1000L);
					} catch (final InterruptedException e) {
						this.finished = true;
						return;
					}
				}
			}
		}
	}
	
	@Override
	public String toString() {
		
		return "[]";
	}
}
