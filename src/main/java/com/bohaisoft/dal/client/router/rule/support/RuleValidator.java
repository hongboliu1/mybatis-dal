/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router.rule.support;

import com.bohaisoft.dal.client.router.config.dataobject.RuleCriteria;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

/**
 * 
 * 
 * @author wuxiang
 * @since 2012-4-5
 */
public class RuleValidator {

	public static<T> void validateShardingRules(Map<String, Collection<T>> shardingRules) {
		int distinctCount = 0;
		Collection<T> distinctRules = new HashSet<T>();
		if(shardingRules != null) {
			for(Collection<T> rules : shardingRules.values()) {
				for(T rule : rules) {
					if(rule == null) {
						continue;
					}
					if(rule instanceof String) {
						if(StringUtils.isEmpty((String)rule)) {
							continue;
						}
					}
					distinctRules.add(rule);
					distinctCount++;
				}
			}
			if(distinctCount != distinctRules.size()) {
				throw new IllegalArgumentException("The property sharding rules mapping of bean 'shardingRuleContainer' contains duplicated values in dal-sharding config file");
			}
		}
	}
	
	public static<T> boolean isInRange(RuleCriteria<T> range, Comparable<T> value) {
		if(value == null) {
			return false;
		}
		if(range.getGe() != null) {
			if(value.compareTo(range.getGe()) < 0) {
				return false;
			}
		}
		if(range.getGt() != null) {
			if(value.compareTo(range.getGt()) <= 0) {
				return false;
			}
		}
		if(range.getLt() != null) {
			if(value.compareTo(range.getLt()) >= 0) {
				return false;
			}
		}
		if(range.getLe() != null) {
			if(value.compareTo(range.getLe()) > 0) {
				return false;
			}
		}
		if(range.getGe() == null && range.getGt() == null
				&& range.getLt() == null && range.getLe() == null) {
			return false;
		}
		return true;
	}
}
