/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router.config.dataobject;


import org.apache.commons.lang3.StringUtils;

/**
 * 数据源配置
 * 
 * @author wuxiang
 * @since 2012-5-11
 */
public class DataSourceConfig {

	private String groupDS;//所属数据源组id
	private String slaveDS;//所属slave数据源id
	private Boolean isMaster;//是否走主库

	public Boolean getIsMaster() {
		return isMaster;
	}

	public void setIsMaster(Boolean isMaster) {
		this.isMaster = isMaster;
	}

	public String getGroupDS() {
		return groupDS;
	}

	public void setGroupDS(String groupDS) {
		if (StringUtils.isEmpty(groupDS)) {
			throw new IllegalArgumentException("The 'groupDS' property of 'sqlIdDatasource' in 'dal-sqlconfig' xml is empty");
		}
		this.groupDS = groupDS;
	}

	public String getSlaveDS() {
		return slaveDS;
	}

	public void setSlaveDS(String slaveDS) {
		if (StringUtils.isEmpty(slaveDS)) {
			throw new IllegalArgumentException("The 'slaveDS' property of 'sqlIdDatasource' in 'dal-sqlconfig' xml is empty");
		}
		this.slaveDS = slaveDS;
	}

}
