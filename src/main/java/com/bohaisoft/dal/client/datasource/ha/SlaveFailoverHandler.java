/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */
package com.bohaisoft.dal.client.datasource.ha;

import javax.sql.DataSource;

import com.bohaisoft.dal.client.datasource.AtomDS;
import com.bohaisoft.dal.client.datasource.GroupDS;
import com.bohaisoft.dal.client.datasource.MasterDS;
import com.bohaisoft.dal.client.datasource.reload.IDataSourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.target.HotSwappableTargetSource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.util.CollectionUtils;


/**
 * 
 * 
 * @author wuxiang
 * @since 2012-4-13
 */
public class SlaveFailoverHandler implements IFailoverHandler {

	static final Logger log = LoggerFactory.getLogger(SlaveFailoverHandler.class);

	@Override
	public DataSource handleUnavailable(GroupDS groupDS, HotSwappableTargetSource targetSource, DataSource masterDataSource, DataSource standbyDataSource) {
		DataSource activeDataSource = (DataSource) targetSource.getTarget();
		groupDS.disableSlave(activeDataSource);// remove this data source from
												// slave data sources of groupDS

		if (groupDS.isReloadDataSourceWhileFailure()) {// 重启数据源
			IDataSourceLoader loader = groupDS.getDataSourceLoader();
			loader.reload(activeDataSource);
		}

		MasterDS masterDS = groupDS.getMaster();
		if (!CollectionUtils.isEmpty(groupDS.getMapToSlaveDS())) {
			AtomDS slaveDS = groupDS.getMapToSlaveDS().values().iterator().next();
			return slaveDS.getTargetDataSource();
		}
		if (masterDS != null) {
			if (masterDS.getMaster() != null && activeDataSource != masterDS.getMaster().getOriginalDataSource()) {
				return masterDS.getMaster().getOriginalDataSource();
			}
			if (masterDS.getHotBackup() != null && activeDataSource != masterDS.getHotBackup().getOriginalDataSource()) {
				return masterDS.getHotBackup().getOriginalDataSource();
			}
		}
		throw new DataAccessResourceFailureException("failed to get Connection from data source:" + activeDataSource);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.yihaodian.ydal.client.datasource.ha.IFailoverHandler#
	 * determineCurrentDetectorDataSource
	 * (com.yihaodian.ydal.client.datasource.GroupDS,
	 * com.yihaodian.ydal.client.datasource.AtomDS)
	 */
	@Override
	public AtomDS determineCurrentDetectorDataSource(GroupDS groupDS, AtomDS currentDetectorDataSource) {
		return currentDetectorDataSource;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.yihaodian.ydal.client.datasource.ha.IFailoverHandler#resume(com.yihaodian
	 * .ydal.client.datasource.GroupDS,
	 * org.springframework.aop.target.HotSwappableTargetSource)
	 */
	@Override
	public DataSource handleAvailable(GroupDS groupDS, HotSwappableTargetSource targetSource) {
		DataSource activeDataSource = (DataSource) targetSource.getTarget();
		groupDS.enableSlave(activeDataSource);// remove this data source from
												// slave data sources of groupDS

		return (DataSource) targetSource.getTarget();
	}

}
