/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client-1.1.0
 */
package com.bohaisoft.dal.client.cache;


import com.bohaisoft.dal.client.cache.simple.SimpleCacheManager;
import com.bohaisoft.dal.client.support.utils.RandomUtils;

/**
 * 
 * 
 * @author wuxiang
 * @since 2013-3-13
 */
public class CacheFactory implements ICacheManager {

	public static CacheFactory INSTANCE = new CacheFactory();
	private ICacheManager cacheManager = null;
	
	public CacheFactory() {
		selectCacheManager();
	}

	public void selectCacheManager() {
		cacheManager = new SimpleCacheManager();
	}
	
	public synchronized ICache createCache(String name) {
		return createCache(name, 0);
	}
	
	public synchronized ICache createCache(int size) {
		return createCache(RandomUtils.newRandomString(8), size);
	}
	
	public synchronized ICache createCache(String name, int size) {
		ICache cache = getCache(name);
		if(cache == null) {
			return cacheManager.createCache(name, size);
		} else {
			return cache;
		}
	}
	
	/* (non-Javadoc)
	 * @see com.yihaodian.ydal.client.cache.ICacheManager#getCache(java.lang.String)
	 */
	@Override
	public ICache getCache(String key) {
		return cacheManager.getCache(key);
	}

	/* (non-Javadoc)
	 * @see com.yihaodian.ydal.client.cache.ICacheManager#removeCache(java.lang.String)
	 */
	@Override
	public synchronized ICache removeCache(String key) {
		return cacheManager.removeCache(key);
	}

}
