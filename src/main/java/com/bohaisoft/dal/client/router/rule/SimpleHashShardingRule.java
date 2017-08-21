/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router.rule;

import com.bohaisoft.dal.client.exception.RoutingException;
import com.bohaisoft.dal.client.router.rule.support.ShardingRuleMatcher;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;

/**
 *
 *	
 * @author wuxiang
 * @since 2012-3-12
 */
public class SimpleHashShardingRule extends AbstractShardingRule implements InitializingBean {

	private String ruleExp;
	
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
		if(result != null) {
			if(result.equals(ShardingRuleMatcher.ALL_SHARDS)) {
				return getShardsString();
			}
			return result.toString();
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
		super.afterPropertiesSet();
	}
}
