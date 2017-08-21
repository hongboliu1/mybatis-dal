/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router.rule;

import java.util.Collection;
import java.util.Map;

import com.bohaisoft.dal.client.exception.RoutingException;
import com.bohaisoft.dal.client.router.config.dataobject.RuleCriteria;
import com.bohaisoft.dal.client.router.rule.support.RuleValidator;
import com.bohaisoft.dal.client.router.rule.support.ShardingRuleMatcher;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;


/**
 * 按范围进行查找匹配的分库分表规则
 * 
 * @author wuxiang
 * @since 2012-4-5
 */
public class RangeLookupShardingRule<T> extends AbstractShardingRule implements InitializingBean {

	private String ruleExp;
	private Log log = LogFactory.getLog(RangeLookupShardingRule.class);

	// mapping for sharding target and sharding key set
	// Map key-sharding target(e.g. datasource group id:group1,group2,group3)
	// Map value-sharding key set(e.g. "merchantId" value:1,2,3)
	private Map<String, RuleCriteria<T>> lookupRules = null;

	/**
	 * @return the lookupRules
	 */
	public Map<String, RuleCriteria<T>> getLookupRules() {
		return lookupRules;
	}

	/**
	 * @param lookupRules
	 *            the lookupRules to set
	 */
	public void setLookupRules(Map<String, RuleCriteria<T>> lookupRules) {
		this.lookupRules = lookupRules;
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
		if ((parameterObject instanceof Collection) || parameterObject != null && parameterObject.getClass().isArray()) {//暂不支持集合类的lookup
			log.warn("Lookup sharding rule does not support parameter of Collection or array:" + parameterObject);
			return null;
		}
		Object result = match(ruleExp, parameterObject);

		if (result instanceof Comparable && lookupRules != null) {
			if(result.equals(ShardingRuleMatcher.ALL_SHARDS)) {
				return getShardsString();
			}
			Comparable<T> value = (Comparable<T>) result;
			for (String group : lookupRules.keySet()) {
				RuleCriteria<T> range = lookupRules.get(group);
				if (RuleValidator.isInRange(range, value)) {
					return group;
				}
			}
		}

		return null;
	}

	public String toString() {
		return ruleExp;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (StringUtils.isEmpty(ruleExp)) {
			throw new IllegalArgumentException("The ruleExp property must be set");
		}
		if (CollectionUtils.isEmpty(lookupRules)) {
			throw new IllegalArgumentException("The lookupRules property must be set");
		}
		
		super.afterPropertiesSet();
	}

}
