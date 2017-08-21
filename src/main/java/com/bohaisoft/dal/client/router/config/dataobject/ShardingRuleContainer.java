/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router.config.dataobject;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 *	
 * @author wuxiang
 * @since 2012-3-9
 */
public class ShardingRuleContainer {

	private Map<String, ShardingRuleOfTable> horizontalShardingRules = null;

	//mapping for datasource group and sharding table set
	//Map key-datasource group name(e.g. group1,group2,group3)
	//Map value-table name set
	private Map<String, Collection<String>> verticalShardingRules = null;
	private boolean ignoreCase = true;

	/**
	 * @return the ignoreCase
	 */
	public boolean isIgnoreCase() {
		return ignoreCase;
	}

	/**
	 * @param ignoreCase the ignoreCase to set
	 */
	public void setIgnoreCase(boolean ignoreCase) {
		this.ignoreCase = ignoreCase;
	}

	public Map<String, ShardingRuleOfTable> getHorizontalShardingRules() {
		return horizontalShardingRules;
	}

	public void setHorizontalShardingRules(Map<String, ShardingRuleOfTable> horizontalShardingRules) {
		if(ignoreCase && horizontalShardingRules != null) {
			Map<String, ShardingRuleOfTable> rules = new HashMap<String, ShardingRuleOfTable>();
			for(String key : horizontalShardingRules.keySet()) {
				ShardingRuleOfTable rule = horizontalShardingRules.get(key);
				rules.put(key.toUpperCase(), rule);
			}
			this.horizontalShardingRules = rules;
		} else {
			this.horizontalShardingRules = horizontalShardingRules;
		}
	}

	public Map<String, Collection<String>> getVerticalShardingRules() {
		return verticalShardingRules;
	}

	public void setVerticalShardingRules(Map<String, Collection<String>> verticalShardingRules) {
		if(ignoreCase && verticalShardingRules != null) {
			Map<String, Collection<String>> rules = new HashMap<String, Collection<String>>();
			for(String key : verticalShardingRules.keySet()) {
				Collection<String> values = verticalShardingRules.get(key);
				List<String> isnoreCaseValues = new ArrayList<String>();
				for(String str : values) {
					if(StringUtils.isNotEmpty(str)) {
						isnoreCaseValues.add(str.toUpperCase());
					}
				}
				rules.put(key, isnoreCaseValues);
			}
			this.verticalShardingRules = rules;
		} else {
			this.verticalShardingRules = verticalShardingRules;
		}
	}
		
}
