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
public class MergedSql {
	SqlIteratorType sqlIteratorType;
	String tailSql;

	public SqlIteratorType getSqlIteratorType() {
		return sqlIteratorType;
	}

	public void setSqlIteratorType(SqlIteratorType sqlIteratorType) {
		this.sqlIteratorType = sqlIteratorType;
	}

	public String getTailSql() {
		return tailSql;
	}

	public void setTailSql(String tailSql) {
		this.tailSql = tailSql;
	}

	public String toString() {
		return sqlIteratorType + "," + tailSql;
	}
}
