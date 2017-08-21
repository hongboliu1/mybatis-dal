/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client-1.1.0
 */
package com.bohaisoft.dal.client.router.config.dataobject;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;
import java.util.Map;


/**
 * 
 * 
 * @author wuxiang
 * @since 2013-1-6
 */
public class ShardingMapping implements Comparable<ShardingMapping> {

	private Map<String, String> virtualTablesMapping;

	private List<Object> parameterObjects;
	
	private List<Object> initialParameterObjects;
	
	public List<Object> getInitialParameterObjects() {
		return initialParameterObjects;
	}

	public void setInitialParameterObjects(List<Object> initialParameterObjects) {
		this.initialParameterObjects = initialParameterObjects;
	}

	private boolean multiple = false;

	public boolean isMultiple() {
		return multiple;
	}

	public void setMultiple(boolean multiple) {
		this.multiple = multiple;
	}

	public Map<String, String> getVirtualTablesMapping() {
		return virtualTablesMapping;
	}

	public void setVirtualTablesMapping(Map<String, String> virtualTablesMapping) {
		this.virtualTablesMapping = virtualTablesMapping;
	}

	public List<Object> getParameterObjects() {
		return parameterObjects;
	}

	public void setParameterObjects(List<Object> parameterObjects) {
		this.parameterObjects = parameterObjects;
	}

	public int hashCode() {
		return new HashCodeBuilder().append(virtualTablesMapping).toHashCode();
	}

	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		ShardingMapping an = (ShardingMapping) obj;
		return new EqualsBuilder().append(virtualTablesMapping, an.getVirtualTablesMapping()).isEquals();
	}

	public String toString() {
		return "virtual tables mapping:" + virtualTablesMapping;
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(ShardingMapping o) {
		if(o == null) {
			return -1;
		} else if(o.getVirtualTablesMapping() != null && this.getVirtualTablesMapping() != null){
			for(String key : this.getVirtualTablesMapping().keySet()) {
				String value1 = this.getVirtualTablesMapping().get(key);
				String value2 = o.getVirtualTablesMapping().get(key);
				if(value1 != null && value2 != null) {
					return value1.compareTo(value2);
				}
			}
		}
		return 0;
	}
}
