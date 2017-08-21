/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */
package com.bohaisoft.dal.client.router.sql;

/**
 * 
 * 
 * @author wuxiang
 * @since 2013-7-31
 */
public class ReplacedSql {
	String sql;
	int parametersLength;

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public int getParametersLength() {
		return parametersLength;
	}

	public void setParametersLength(int parametersLength) {
		this.parametersLength = parametersLength;
	}

}
