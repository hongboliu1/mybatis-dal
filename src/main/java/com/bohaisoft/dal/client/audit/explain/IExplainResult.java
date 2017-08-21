/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */
package com.bohaisoft.dal.client.audit.explain;

/**
 * 
 * 
 * @author wuxiang
 * @since 2012-8-10
 */
public interface IExplainResult {

	public String getType();
	
	public String getDetail();
	
	public void setBadSql(boolean badSql);
	
	public boolean isBadSql();
	
}
