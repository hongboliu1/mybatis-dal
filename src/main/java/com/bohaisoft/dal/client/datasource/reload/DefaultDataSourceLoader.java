/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.datasource.reload;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

/**
 * 
 * @author wuxiang
 * @since 2012-6-11
 */
public class DefaultDataSourceLoader implements IDataSourceLoader {

	private transient Logger logger = LoggerFactory.getLogger(DefaultDataSourceLoader.class);

	public static final IDataSourceLoader INSTANCE = new DefaultDataSourceLoader();

	private DefaultDataSourceLoader() {

	}

	public synchronized void reload(DataSource dataSource) {
		try {
			if (dataSource instanceof IReloadableDatasource) {
				((IReloadableDatasource) dataSource).reload();

			} else if (dataSource instanceof FactoryBean) {
				FactoryBean bean = (FactoryBean) dataSource;
				Object target = bean.getObject();
				if (target instanceof DruidDataSource) {
					reloadDbcpDataSource((DruidDataSource) target);
				}
			} else if (dataSource instanceof DruidDataSource) {
				reloadDbcpDataSource((DruidDataSource) dataSource);
			}
		} catch (Throwable e) {
			logger.error("Error while reloading data source", e);
		}
	}

	private void reloadDbcpDataSource(DruidDataSource dataSource) throws Exception {
		dataSource.close();
	}
}
