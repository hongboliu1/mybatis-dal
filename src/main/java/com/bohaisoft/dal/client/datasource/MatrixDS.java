package com.bohaisoft.dal.client.datasource;

import java.util.HashMap;
import java.util.Map;

/**
 * 路由具体的主库或备库数据源
 * @author duxinyun
 */
public class MatrixDS {
	
	/**Matrix 拥有多个group 组 */
	private Map<String,GroupDS> groupMap;
	private boolean isDefaultMaster = true;//默认操作主库还是备库
	private GroupDS defaultGroupDS;//默认数据源组
	
	/**在MatrixDS 中注入groupMap 
	 * 数据来源 可见spring-ydal-groupdatasource.xml的bean id= matrixDS
	 * key: groupId
	 * value: GroupDS 
     */
	public Map<String, GroupDS> getGroupMap() {
		if(groupMap==null){
			return new HashMap<String, GroupDS>();
		}
		return groupMap;
	}

	public void setIsDefaultMaster(boolean isDefaultMaster) {
	    this.isDefaultMaster = isDefaultMaster;
	}

	public void setGroupMap(Map<String, GroupDS> groupMap) {
		this.groupMap = groupMap;
	}

	public boolean isDefaultMaster() {
		return isDefaultMaster;
	}


	public GroupDS getDefaultGroupDS() {
		return defaultGroupDS;
	}

	public void setDefaultGroupDS(GroupDS defaultGroupDS) {
		this.defaultGroupDS = defaultGroupDS;
	}

}
