/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router.rule;

import java.util.Set;

import com.bohaisoft.dal.client.exception.RoutingException;
import com.bohaisoft.dal.client.router.rule.hash.ConsistentHash;
import com.bohaisoft.dal.client.router.rule.hash.HashFunction;
import com.bohaisoft.dal.client.router.rule.hash.MD5HashFunction;
import com.bohaisoft.dal.client.router.rule.support.ShardingRuleMatcher;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;

/**
 * 
 * 
 * @author wuxiang
 * @since 2012-3-12
 */
public class ConsistentHashShardingRule extends AbstractShardingRule implements InitializingBean {

	private String ruleExp;
	private Set<String> nodes = null;
	private int numberOfReplicas = 160;
	private ConsistentHash<String> hash = null;
	private HashFunction hashFun = null;
	private HashFunction nodeHashFun = null;
	
	public HashFunction getHashFun() {
		return hashFun;
	}

	public void setHashFun(HashFunction hashFun) {
		this.hashFun = hashFun;
	}

	public HashFunction getNodeHashFun() {
		return nodeHashFun;
	}

	public void setNodeHashFun(HashFunction nodeHashFun) {
		this.nodeHashFun = nodeHashFun;
	}

	public Set<String> getNodes() {
		return nodes;
	}

	public void setNodes(Set<String> nodes) {
		this.nodes = nodes;
	}

	public int getNumberOfReplicas() {
		return numberOfReplicas;
	}

	public void setNumberOfReplicas(int numberOfReplicas) {
		this.numberOfReplicas = numberOfReplicas;
	}

	/**
	 * @return the ruleExp
	 */
	public String getRuleExp() {
		return ruleExp;
	}

	/**
	 * @param ruleExp
	 *            the ruleExp to set
	 */
	public void setRuleExp(String ruleExp) {
		this.ruleExp = ruleExp;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.yihaodian.ydal.client.router.sharding.ShardingRule#decideDatasourceGroup
	 * (java.util.Map)
	 */
	@Override
	public String decideTargetShards(Object parameterObject) throws RoutingException {
		Object result = match(ruleExp, parameterObject);
		if (result != null) {
			if(result.equals(ShardingRuleMatcher.ALL_SHARDS)) {
				return getShardsString();
			}
			String shard = hash.get(result);
			return shard;
		}

		return null;
	}

	public String toString() {
		return ruleExp;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (StringUtils.isEmpty(ruleExp)) {
			throw new IllegalArgumentException("The 'ruleExp' property must be set");
		}
		if (CollectionUtils.isEmpty(nodes)) {
			throw new IllegalArgumentException("The 'nodes' property must be set");
		}
		if (numberOfReplicas <= 0) {
			throw new IllegalArgumentException("The 'numberOfReplicas' property should have a positive value");
		}
		if (hashFun == null) {
			hashFun = new MD5HashFunction();
		}
		if (nodeHashFun == null) {
			nodeHashFun = hashFun;
		}
		if (hash == null) {
			hash = new ConsistentHash<String>(nodeHashFun, hashFun, numberOfReplicas, nodes);
		}

		super.afterPropertiesSet();
	}

}
