package ru.myx.bdbje.util;

import com.sleepycat.je.OperationResult;

class Record {

	byte[] key;

	byte[] data;
	
	OperationResult result;

	public Record(final byte[] key, final byte[] data, final OperationResult result) {

		this.key = key;
		this.data = data;
		this.result = result;
	}
}