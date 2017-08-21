package com.bohaisoft.dal.client.router.support;

import com.bohaisoft.dal.client.router.config.dataobject.ShardingMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Created by IntelliJ IDEA. User: Administrator Date: 12-2-24 Time: 下午1:12 To
 * change this template use File | Settings | File Templates.
 */
public class RouterFactCtxVO {

	private String statementName;
	private Boolean mdbFlag;
	private String groupDSName;// 操作的数据源组名称
	private List<ShardingMapping> virtualTablesMapping;// 虚拟表名与实际分表名称映射
	private Map<RouterFact, Boolean> keepInThreadFacts = null;// 在一个线程中保持不变的设置
	private Object parameters;
	private Boolean executeQueryInConcurrency;
	private Boolean executeInsertInConcurrency;
	private Boolean executeUpdateInConcurrency;
	private Boolean executeDeleteInConcurrency;

	public String getStatementName() {
		return statementName;
	}

	public void setStatementName(String statementName) {
		this.statementName = statementName;
	}

	public Boolean getExecuteQueryInConcurrency() {
		return executeQueryInConcurrency;
	}

	public void setExecuteQueryInConcurrency(Boolean executeQueryInConcurrency) {
		this.executeQueryInConcurrency = executeQueryInConcurrency;
	}

	public Boolean getExecuteInsertInConcurrency() {
		return executeInsertInConcurrency;
	}

	public void setExecuteInsertInConcurrency(Boolean executeInsertInConcurrency) {
		this.executeInsertInConcurrency = executeInsertInConcurrency;
	}

	public Boolean getExecuteUpdateInConcurrency() {
		return executeUpdateInConcurrency;
	}

	public void setExecuteUpdateInConcurrency(Boolean executeUpdateInConcurrency) {
		this.executeUpdateInConcurrency = executeUpdateInConcurrency;
	}

	public Boolean getExecuteDeleteInConcurrency() {
		return executeDeleteInConcurrency;
	}

	public void setExecuteDeleteInConcurrency(Boolean executeDeleteInConcurrency) {
		this.executeDeleteInConcurrency = executeDeleteInConcurrency;
	}

	public Object getParameters() {
		return parameters;
	}

	public void setParameters(Object parameters) {
		this.parameters = parameters;
	}

	public static enum RouterFact {
		IS_MASTER, GROUPDS, VIRTUAL_TABLES_MAPPING, EXECUTE_QUERY_IN_CONCURRENCY, EXECUTE_UPDATE_IN_CONCURRENCY, EXECUTE_INSERT_IN_CONCURRENCY, EXECUTE_DELETE_IN_CONCURRENCY
	}

	public List<ShardingMapping> getVirtualTablesMapping() {
		return virtualTablesMapping;
	}

	public void setVirtualTablesMapping(List<ShardingMapping> virtualTablesMapping) {
		this.virtualTablesMapping = virtualTablesMapping;
	}

	public void clearVirtualTablesMapping() {
		this.virtualTablesMapping = null;
	}

	public void setVirtualTablesMapping(Map<String, String> virtualTablesMapping) {
		List<ShardingMapping> mappings = new ArrayList<ShardingMapping>();
		ShardingMapping mapping = new ShardingMapping();
		mapping.setVirtualTablesMapping(virtualTablesMapping);
		this.virtualTablesMapping = mappings;
	}

	public String getGroupDSName() {
		return groupDSName;
	}

	public void setGroupDSName(String groupDSName) {
		this.groupDSName = groupDSName;
	}

	public Boolean getMdbFlag() {
		return mdbFlag;
	}

	public void setMdbFlag(Boolean mdbFlag) {
		this.mdbFlag = mdbFlag;
	}

	public Map<RouterFact, Boolean> getKeepInThreadFacts() {
		return keepInThreadFacts;
	}

	public void setKeepInThreadFacts(Map<RouterFact, Boolean> keepInThreadFacts) {
		this.keepInThreadFacts = keepInThreadFacts;
	}

	public String toString() {
		return "group-" + groupDSName + ",virtualTablesMapping-" + virtualTablesMapping;
	}
}
