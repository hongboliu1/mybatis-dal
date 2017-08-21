/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */
package com.bohaisoft.dal.client.datasource.ha;

import javax.sql.DataSource;

import com.bohaisoft.dal.client.datasource.AtomDS;
import com.bohaisoft.dal.client.datasource.GroupDS;
import org.springframework.aop.target.HotSwappableTargetSource;


/**
 * 
 * 
 * @author wuxiang
 * @since 2012-4-13
 */
public interface IFailoverHandler {

	public DataSource handleUnavailable(GroupDS groupDS, HotSwappableTargetSource targetSource, DataSource masterDataSource, DataSource standbyDataSource);
	
	public DataSource handleAvailable(GroupDS groupDS, HotSwappableTargetSource targetSource);
	
	public AtomDS determineCurrentDetectorDataSource(GroupDS groupDS, AtomDS currentDetectorDataSource);
}
