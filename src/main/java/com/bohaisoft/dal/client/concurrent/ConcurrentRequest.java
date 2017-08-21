/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client-1.1.0
 */
package com.bohaisoft.dal.client.concurrent;


import com.bohaisoft.dal.client.router.config.dataobject.ShardingMapping;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * 并发请求接口
 * @author wuxiang
 * @since 2013-1-7
 */
public class ConcurrentRequest {
	private DefaultSqlMapClientCallback action;
	private DataSource dataSource;
	private ExecutorService executor;
	private List<ShardingMapping> shardingMappingSet;
	private Object parameters;
	String statementName;

	public String getStatementName() {
		return statementName;
	}

	public void setStatementName(String statementName) {
		this.statementName = statementName;
	}

	public Object getParameters() {
		return parameters;
	}

	public void setParameters(Object parameters) {
		this.parameters = parameters;
	}

	public List<ShardingMapping> getShardingMappingSet() {
		return shardingMappingSet;
	}

	public void setShardingMappingSet(List<ShardingMapping> shardingMappingSet) {
		this.shardingMappingSet = shardingMappingSet;
	}

	public DefaultSqlMapClientCallback getAction() {
		return action;
	}

	public void setAction(DefaultSqlMapClientCallback action) {
		this.action = action;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public ExecutorService getExecutor() {
		return executor;
	}

	public void setExecutor(ExecutorService executor) {
		this.executor = executor;
	}

}
