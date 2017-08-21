/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */
package com.bohaisoft.dal.client.support.type;

/**
 * 
 * 
 * @author wuxiang
 * @since 2013-6-30
 */
public enum StatementType {
	
	SELECT("select"), INSERT("insert"), UPDATE("update"), DELETE("delete"), EXECUTE("execute");

	String type = null;

	StatementType(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}
}
