package com.bohaisoft.dal.client.router.rule.hash;

public class HashHelper {

	private static HashFunction md5Hash = null; 
	
	public static Object md5(Object key) {
		if(md5Hash == null) {
			md5Hash = new MD5HashFunction();
		}
		return md5Hash.hash(key);
	}
}
