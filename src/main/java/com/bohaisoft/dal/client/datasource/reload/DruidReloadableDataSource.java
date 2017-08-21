/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.datasource.reload;

import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author wuxiang
 * @since 2012-6-11
 */
public class DruidReloadableDataSource extends DruidDataSource implements IReloadableDatasource {

	private transient Logger logger = LoggerFactory.getLogger(this.getClass());

	public void reload() {
		try {
			this.close();
		} catch (Exception e) {
			logger.error("Error while reloading data source", e);
		}
	}
}
