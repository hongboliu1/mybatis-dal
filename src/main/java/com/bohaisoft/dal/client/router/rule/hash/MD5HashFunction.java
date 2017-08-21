package com.bohaisoft.dal.client.router.rule.hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class MD5HashFunction implements HashFunction {

	MessageDigest md5 = null;

	@Override
	public Object hash(Object key) {
		if (key == null) {
			return null;
		}
		if (md5 == null) {
			try {
				md5 = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				throw new IllegalStateException("No md5 algorythm found");
			}
		}

		md5.reset();
		md5.update(key.toString().getBytes());
		byte[] bKey = md5.digest();
		long res = ((long) (bKey[3] & 0xFF) << 24) | ((long) (bKey[2] & 0xFF) << 16) | ((long) (bKey[1] & 0xFF) << 8) | (long) (bKey[0] & 0xFF);

		return res;
	}

}
