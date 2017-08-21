/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */
package com.bohaisoft.dal.client.datasource.ha;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.bohaisoft.dal.client.datasource.AtomDS;
import com.bohaisoft.dal.client.datasource.GroupDS;
import com.bohaisoft.dal.client.datasource.MasterDS;
import com.bohaisoft.dal.client.datasource.reload.IDataSourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.target.HotSwappableTargetSource;


/**
 * 
 * 
 * @author wuxiang
 * @since 2012-4-13
 */
public class MasterFailoverHandler implements IFailoverHandler {

	static final Logger log = LoggerFactory.getLogger(MasterFailoverHandler.class);

	public boolean checkAvailable(DataSource dataSource, String detectingSql) {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
			PreparedStatement pstmt = conn.prepareStatement(detectingSql);
			boolean ret = false;
			if(detectingSql.trim().toLowerCase().startsWith("select ")) {
				ResultSet rs = pstmt.executeQuery();
				ret = rs.next();
				rs.close();
			} else {
				ret = pstmt.execute();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			return ret;
		} catch (Exception e) {
			log.warn("failed to check failover data source:" + dataSource + ",detectingSql:" + detectingSql, e);
			return false;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					log.warn("failed to close checking connection:\n", e);
				}
			}
		}
	}
	
	@Override
	public DataSource handleUnavailable(GroupDS groupDS, HotSwappableTargetSource targetSource, DataSource masterDataSource, DataSource standbyDataSource) {
		if (masterDataSource == null) {
			throw new IllegalArgumentException("master data source can't be null.");
		}
		MasterDS masterDS = groupDS.getMaster();
		DataSource target = (DataSource) targetSource.getTarget();
		synchronized (targetSource) {
			if (groupDS.isReloadDataSourceWhileFailure()) {// 重启数据源
				IDataSourceLoader loader = groupDS.getDataSourceLoader();
				loader.reload(target);
			}
			if (standbyDataSource != null) {// 切换数据源
				if (target == masterDataSource) {
					boolean isTargetAvailable = true;
					if (groupDS.isTestTargetDataSourceBeforeFailover()) {
						isTargetAvailable = checkAvailable(standbyDataSource, masterDS.getDetectingSql());
					}
					if(isTargetAvailable) {
						log.warn("hot swap from '" + target + "' to '" + standbyDataSource + "'.");
						targetSource.swap(standbyDataSource);
						masterDS.setCurrentDetector(masterDS.getHotBackupDetector());
						groupDS.disableSlave(standbyDataSource);// remove this data
																// source from slave
																// data sources of
																// groupDS
					}
				} else {
					boolean isTargetAvailable = true;
					if (groupDS.isTestTargetDataSourceBeforeFailover()) {
						isTargetAvailable = checkAvailable(masterDataSource, masterDS.getDetectingSql());
					}
					if(isTargetAvailable) {
						log.warn("hot swap from '" + target + "' to '" + masterDataSource + "'.");
						targetSource.swap(masterDataSource);
						masterDS.setCurrentDetector(masterDS.getMasterDetector());
						groupDS.disableSlave(masterDataSource);// remove this data
																// source from slave
																// data sources of
																// groupDS
					}
				}
			}
		}
		return (DataSource) targetSource.getTarget();
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
		return groupDS.getMaster().getCurrentDetector();
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
		return (DataSource) targetSource.getTarget();
	}

}
