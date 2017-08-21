package com.bohaisoft.dal.client.router.config.dataobject;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Collection;
import java.util.List;



public class ShardingFactDO {
	private Collection<String> tables;
	private Boolean isMaster;
	private String groupDS;
	private String statementName;
	private Object parameterObject;
	List<ShardingMapping> virtualTablesMapping;

	public List<ShardingMapping> getVirtualTablesMapping() {
		return virtualTablesMapping;
	}

	public void setVirtualTablesMapping(List<ShardingMapping> virtualTablesMapping) {
		this.virtualTablesMapping = virtualTablesMapping;
	}

	public String getStatementName() {
		return statementName;
	}

	public void setStatementName(String statementName) {
		this.statementName = statementName;
	}

	public Object getParameterObject() {
		return parameterObject;
	}

	public void setParameterObject(Object parameterObject) {
		this.parameterObject = parameterObject;
	}

	public String getGroupDS() {
		return groupDS;
	}

	public void setGroupDS(String groupDS) {
		this.groupDS = groupDS;
	}

	public Collection<String> getTables() {
		return tables;
	}

	public void setTables(Collection<String> tables) {
		this.tables = tables;
	}

	public Boolean getIsMaster() {
		return isMaster;
	}

	public void setIsMaster(Boolean isMaster) {
		this.isMaster = isMaster;
	}

	public boolean equals(Object an) {
		return EqualsBuilder.reflectionEquals(this, an);
	}

	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this);
	}

	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
