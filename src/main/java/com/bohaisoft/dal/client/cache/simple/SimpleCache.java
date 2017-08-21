/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client-1.1.0
 */
package com.bohaisoft.dal.client.cache.simple;

import com.bohaisoft.dal.client.cache.ICache;

import java.util.Date;


public class SimpleCache implements ICache {

	private SimpleCacheMap<String, SimpleCacheElement> cacheMap = null;
	private final static long defaultTTL = -1;
	private final static int defaultSize = 9999;

	public SimpleCache() {
		cacheMap = new SimpleCacheMap<String, SimpleCacheElement>(defaultSize);
	}
	
	public SimpleCache(int size) {
		if(size <= 0) {
			size = defaultSize;
		}
		cacheMap = new SimpleCacheMap<String, SimpleCacheElement>(size);
	}
	
	private synchronized SimpleCacheElement getCache(String key) {
		return cacheMap.get(key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.yihaodian.ydal.client.cache.ICache#hasCache(java.lang.String)
	 */
	@Override
	public boolean hasKey(String key) {
		return cacheMap.containsKey(key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.yihaodian.ydal.client.cache.ICache#invalidateAll()
	 */
	@Override
	public void invalidateAll() {
		cacheMap.clear();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.yihaodian.ydal.client.cache.ICache#invalidate(java.lang.String)
	 */
	@Override
	public void invalidate(String key) {
		cacheMap.remove(key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.yihaodian.ydal.client.cache.ICache#getContent(java.lang.String)
	 */
	@Override
	public Object getContent(String key) {
		if (hasKey(key)) {
			SimpleCacheElement cache = getCache(key);
			if (cache == null) {
				return null;
			}
			if (cacheExpired(cache)) {
				invalidate(key);
				return null;
			}
			return cache.getValue();
		} else {
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.yihaodian.ydal.client.cache.ICache#putContent(java.lang.String,
	 * java.lang.Object)
	 */
	@Override
	public void putContent(String key, Object content) {
		putContent(key, content, defaultTTL);
	}

	private synchronized void putCache(String key, SimpleCacheElement object) {
		cacheMap.put(key, object);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.yihaodian.ydal.client.cache.ICache#putContent(java.lang.String,
	 * java.lang.Object, long)
	 */
	@Override
	public void putContent(String key, Object content, long ttl) {
		SimpleCacheElement cache = new SimpleCacheElement();
		cache.setKey(key);
		cache.setValue(content);
		if (ttl >= 0) {
			cache.setTimeOut(ttl + new Date().getTime());
		} else {
			cache.setTimeOut(-1);
		}
		cache.setExpired(false);
		putCache(key, cache);
	}

	private static boolean cacheExpired(SimpleCacheElement cache) {
		if (cache == null) {
			return false;
		}
		long milisExpire = cache.getTimeOut();
		if (milisExpire < 0) { // Cache never expires
			return false;
		} else if (new Date().getTime() >= milisExpire) {
			return true;
		} else {
			return false;
		}
	}
}
