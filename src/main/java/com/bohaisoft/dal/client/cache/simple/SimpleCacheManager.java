/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client-1.1.0
 */
package com.bohaisoft.dal.client.cache.simple;

import com.bohaisoft.dal.client.cache.ICache;
import com.bohaisoft.dal.client.cache.ICacheManager;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * 
 * @author wuxiang
 * @since 2013-3-13
 */
public class SimpleCacheManager implements ICacheManager {

	private Map<String, SimpleCache> caches = new HashMap<String, SimpleCache>();
	
	public ICache createCache(String name) {
		return createCache(name, 0);
	}
	
	public ICache createCache(String name, int size) {
		if(caches.containsKey(name)) {
			throw new RuntimeException("existing cache with name:" + name);
		}
		SimpleCache cache = new SimpleCache(size);
		caches.put(name, cache);
		return cache;
	}
	
	/* (non-Javadoc)
	 * @see com.yihaodian.ydal.client.cache.ICacheManager#getCache(java.lang.String)
	 */
	@Override
	public ICache getCache(String key) {
		return caches.get(key);
	}

	/* (non-Javadoc)
	 * @see com.yihaodian.ydal.client.cache.ICacheManager#removeCache(java.lang.String)
	 */
	@Override
	public ICache removeCache(String key) {
		return caches.remove(key);
	}

}
