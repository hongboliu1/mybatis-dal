package com.bohaisoft.dal.client.router.rule;

import java.util.List;

import com.bohaisoft.dal.client.cache.CacheFactory;
import com.bohaisoft.dal.client.cache.ICache;
import com.bohaisoft.dal.client.exception.RoutingException;
import com.bohaisoft.dal.client.router.rule.parser.DefaultFelExpParser;
import com.bohaisoft.dal.client.router.rule.parser.IExpParser;
import com.bohaisoft.dal.client.router.rule.support.ShardingRuleMatcher;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;



public abstract class AbstractShardingRule implements ShardingRule, InitializingBean {

	protected IExpParser expParser;
	protected boolean reflectWithCglib = false;
	protected List<String> shards;
	private String shardsString;
	private int ruleCacheSize = 0;
	private ICache ruleCache = null;
	private ShardingRuleMatcher matcher = new ShardingRuleMatcher();

	public ICache getRuleCache() {
		return ruleCache;
	}

	public void setRuleCache(ICache ruleCache) {
		this.ruleCache = ruleCache;
	}

	public int getRuleCacheSize() {
		return ruleCacheSize;
	}

	public void setRuleCacheSize(int ruleCacheSize) {
		this.ruleCacheSize = ruleCacheSize;
	}

	public List<String> getShards() {
		return shards;
	}

	public void setShards(List<String> shards) {
		this.shards = shards;
	}

	public boolean isReflectWithCglib() {
		return reflectWithCglib;
	}

	public void setReflectWithCglib(boolean reflectWithCglib) {
		this.reflectWithCglib = reflectWithCglib;
	}

	public IExpParser getExpParser() {
		return expParser;
	}

	public void setExpParser(IExpParser expParser) {
		this.expParser = expParser;
	}

	public String getShardsString() {
		if(shardsString == null && !CollectionUtils.isEmpty(shards)) {
			shardsString = StringUtils.join(shards, ",");
		}
		return shardsString;
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		if(ruleCache == null && ruleCacheSize > 0) {
//			String cacheName = this.getClass().getSimpleName() + "-[" + this + "]-" + this.hashCode();
			ruleCache = CacheFactory.INSTANCE.createCache(ruleCacheSize);
		}
		if (expParser == null) {
			expParser = new DefaultFelExpParser();
		}
		matcher.setExpParser(expParser);
		matcher.setReflectWithCglib(reflectWithCglib);
		matcher.setRuleCache(ruleCache);
	}

	protected Object match(String ruleExp, Object parameterObject) throws RoutingException {
		return matcher.match(ruleExp, parameterObject);
	}
	
}
