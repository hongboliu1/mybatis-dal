/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router.config.dataobject;

import com.bohaisoft.dal.client.datasource.AtomDS;
import com.bohaisoft.dal.client.datasource.GroupDS;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Map;

/**
 * 分片结果
 * 
 * @author wuxiang
 * @since 2012-5-7
 */
public class ShardingResult {

	// 虚拟表名与实际表名的映射
	private Map<String, String> virtualTablesMapping;
	// 所属数据源组
	private GroupDS groupDS;

	private AtomDS atomDS;

	public AtomDS getAtomDS() {
		return atomDS;
	}

	public void setAtomDS(AtomDS atomDS) {
		this.atomDS = atomDS;
	}

	public Map<String, String> getVirtualTablesMapping() {
		return virtualTablesMapping;
	}

	public void setVirtualTablesMapping(Map<String, String> virtualTablesMapping) {
		this.virtualTablesMapping = virtualTablesMapping;
	}

	public GroupDS getGroupDS() {
		return groupDS;
	}

	public void setGroupDS(GroupDS groupDS) {
		this.groupDS = groupDS;
	}

	public boolean equals(Object an) {
		return EqualsBuilder.reflectionEquals(this, an);
	}

	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this);
	}

	public String toString() {
		return "groupDS=" + groupDS + ",virtualTablesMapping=" + virtualTablesMapping;
	}
}
