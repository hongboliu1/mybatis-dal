/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client-1.1.0
 */
package com.bohaisoft.dal.client.cache;


/**
 * 
 * 
 * @author wuxiang
 * @since 2013-3-13
 */
public interface ICacheManager {

	public ICache createCache(String name);
	
	public ICache createCache(String name, int size);
	
	public ICache getCache(String key);
	
	public ICache removeCache(String key);
}
