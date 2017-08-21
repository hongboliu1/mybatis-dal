/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router.rule.hash;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 一致性hash
 * 
 * @author wuxiang
 * @since 2012-3-21
 */
public class ConsistentHash<T> {
	private final HashFunction hashFunction;
	private final HashFunction nodeHashFunction;
	private final int numberOfReplicas;
	private final SortedMap<Object, T> circle = new TreeMap<Object, T>();

	public ConsistentHash(HashFunction nodeHashFunction, HashFunction hashFunction, int numberOfReplicas, Collection<T> nodes) {
		if(nodeHashFunction == null) {
			this.nodeHashFunction = hashFunction;
		} else {
			this.nodeHashFunction = nodeHashFunction;
		}
		this.hashFunction = hashFunction;
		this.numberOfReplicas = numberOfReplicas;
		for (T node : nodes) {
			add(node);
		}
	}
	
	public ConsistentHash(HashFunction hashFunction, int numberOfReplicas, Collection<T> nodes) {
		this.nodeHashFunction = hashFunction;
		this.hashFunction = hashFunction;
		this.numberOfReplicas = numberOfReplicas;
		for (T node : nodes) {
			add(node);
		}
	}

	public void add(T node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			circle.put(nodeHashFunction.hash(node.toString() + i), node);
		}
	}

	public void remove(T node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			circle.remove(nodeHashFunction.hash(node.toString() + i));
		}
	}

	public T get(Object key) {
		if (circle.isEmpty()) {
			return null;
		}
		Object hash = hashFunction.hash(key);
		if (!circle.containsKey(hash)) {
			SortedMap<Object, T> tailMap = circle.tailMap(hash);
			hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(hash);
	}
}
