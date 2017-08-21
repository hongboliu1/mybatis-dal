/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router.rule;

import java.util.Collection;
import java.util.Map;

import com.bohaisoft.dal.client.exception.RoutingException;
import com.bohaisoft.dal.client.router.rule.support.RuleValidator;
import com.bohaisoft.dal.client.router.rule.support.ShardingRuleMatcher;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;


/**
 *
 *	
 * @author wuxiang
 * @since 2012-3-12
 * @deprecated 使用ListLookupShardingRule替代
 * @see
 */
@Deprecated
public class SimpleLookupShardingRule extends AbstractShardingRule implements InitializingBean {

	private String ruleExp;
	
	//mapping for sharding target and sharding key set
	//Map key-sharding target(e.g. datasource group id:group1,group2,group3)
	//Map value-sharding key set(e.g. "merchantId" value:1,2,3)
	private Map<String, Collection<String>> lookupRules = null;

	/**
	 * @return the lookupRules
	 */
	public Map<String, Collection<String>> getLookupRules() {
		return lookupRules;
	}

	/**
	 * @param lookupRules the lookupRules to set
	 */
	public void setLookupRules(Map<String, Collection<String>> lookupRules) {
		this.lookupRules = lookupRules;
	}

	/**
	 * @return the ruleExp
	 */
	public String getRuleExp() {
		return ruleExp;
	}

	/**
	 * @param ruleExp the ruleExp to set
	 */
	public void setRuleExp(String ruleExp) {
		this.ruleExp = ruleExp;
	}
	
	/* (non-Javadoc)
	 * @see com.yihaodian.ydal.client.router.sharding.ShardingRule#decideDatasourceGroup(java.util.Map)
	 */
	@Override
	public String decideTargetShards(Object parameterObject)
			throws RoutingException {
		Object result = match(ruleExp, parameterObject);
		String value = null;
		if(result != null && lookupRules != null) {
			value = result.toString();
			if(result.equals(ShardingRuleMatcher.ALL_SHARDS)) {
				return getShardsString();
			}
			for(String group : lookupRules.keySet()) {
				Collection<String> values = lookupRules.get(group);
				if(values != null && values.contains(value)) {
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
		//校验lookup规则
		RuleValidator.validateShardingRules(lookupRules);
		
		super.afterPropertiesSet();
	}

}
