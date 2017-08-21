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
public interface ICache {
	
	public boolean hasKey(String key);

	/**
	 * Invalidates all cache
	 */
	public void invalidateAll();

	/**
	 * Invalidates a single cache item
	 * 
	 * @param key
	 */
	public void invalidate(String key);

	/**
	 * Reads a cache item's content
	 * 
	 * @param key
	 * @return
	 */
	public Object getContent(String key);

	public void putContent(String key, Object content);

	/**
	 * 
	 * @param key
	 * @param content
	 * @param ttl
	 */
	public void putContent(String key, Object content, long ttl);
}
