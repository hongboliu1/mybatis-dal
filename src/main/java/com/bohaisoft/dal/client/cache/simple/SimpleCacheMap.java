package com.bohaisoft.dal.client.cache.simple;

import java.util.LinkedHashMap;

public class SimpleCacheMap<K, V> extends LinkedHashMap<K, V> {

	private static final long serialVersionUID = -3700466745992492679L;

	private int coreSize;

	public SimpleCacheMap(int coreSize) {
		super(coreSize + 1, 1.1f, true);
		this.coreSize = coreSize;
	}

	@Override
	protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
		return size() > coreSize;
	}
}
