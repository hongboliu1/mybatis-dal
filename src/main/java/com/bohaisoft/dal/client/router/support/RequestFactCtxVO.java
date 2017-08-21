package com.bohaisoft.dal.client.router.support;

import com.bohaisoft.dal.client.datasource.GroupDS;

import java.util.Map;

public class RequestFactCtxVO {
	
	private Map<GroupDS, String> groupSlavesMapping;//数据源组访问的备库名称映射，确保同一个线程内访问一个数据源组是同一个备库
	private String dataSourceId;
	
	public String getDataSourceId() {
		return dataSourceId;
	}

	public void setDataSourceId(String dataSourceId) {
		this.dataSourceId = dataSourceId;
	}

	public Map<GroupDS, String> getGroupSlavesMapping() {
		return groupSlavesMapping;
	}

	public void setGroupSlavesMapping(Map<GroupDS, String> groupSlavesMapping) {
		this.groupSlavesMapping = groupSlavesMapping;
	}
	
}
