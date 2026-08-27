package ru.myx.bdbje.util;

/** BDB JE related utils
 *
 * @author myx */
public class StaticUtil {

	/** @param bytes1
	 * @param bytes2
	 * @return */
	public static boolean bytesEqual(final byte[] bytes1, final byte[] bytes2) {

		final int length = bytes1.length;
		if (length != bytes2.length) {
			return false;
		}
		for (int i = 0; i < length; ++i) {
			if (bytes1[i] != bytes2[i]) {
				return false;
			}
		}
		return true;
	}
	
	/** Makes lowest next key
	 *
	 * @param key
	 * @param canModify
	 * @return */
	public static final byte[] nextKey(final byte[] key, final boolean canModify) {

		for (int i = key.length; --i >= 0;) {
			if ((0xff & key[i]) < 255) {
				if (canModify) {
					++key[i];
					return key;
				}
				final byte[] result = new byte[key.length];
				System.arraycopy(key, 0, result, 0, key.length);
				++result[i];
				return result;
			}
		}
		{
			final byte[] result = new byte[key.length + 1];
			System.arraycopy(key, 0, result, 0, key.length);
			return result;
		}
	}
}
