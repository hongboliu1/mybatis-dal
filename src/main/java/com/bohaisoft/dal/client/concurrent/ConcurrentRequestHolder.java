/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */
package com.bohaisoft.dal.client.concurrent;

import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * 
 * @author wuxiang
 * @since 2013-7-30
 */
public class ConcurrentRequestHolder {
	static final Logger logger = LoggerFactory.getLogger(ConcurrentRequestHolder.class);

	private Map<Connection, AtomicInteger> connectionReferences = new HashMap<Connection, AtomicInteger>();

	private List<ConcurrentRequestWrapper> requestWrappers = new ArrayList<ConcurrentRequestWrapper>();

	public void addRequest(ConcurrentRequestWrapper requestWrapper) {
		Connection connection = requestWrapper.getConnection();
		if (connection != null) {
			AtomicInteger num = connectionReferences.get(connection);
			if (num == null) {
				connectionReferences.put(connection, new AtomicInteger(1));
			} else {
				num.incrementAndGet();
			}
		}
		requestWrappers.add(requestWrapper);
	}

	public void releaseRequest(ConcurrentRequestWrapper requestWrapper) {
		Connection connection = requestWrapper.getConnection();
		try {
			if (!connection.isClosed()) {
				AtomicInteger num = connectionReferences.get(connection);
				int result = 0;
				if (num != null) {
					result = num.decrementAndGet();
				}
				if (result == 0) {
					boolean transactionAware = requestWrapper.isTransactionAware();
					try {
						if (transactionAware) {
							connection.close();
						} else {
							final DataSource dataSource = requestWrapper.getOriginalRequest().getDataSource();
							DataSourceUtils.doReleaseConnection(connection, dataSource);
						}
					} catch (Throwable ex) {
						logger.info("Could not close JDBC Connection", ex);
					}
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.info("Could not close JDBC Connection", e);
		}
	}

	public void releaseAllRequests() {
		for (ConcurrentRequestWrapper requestWrapper : requestWrappers) {
			Connection connection = requestWrapper.getConnection();
			try {
				if (!connection.isClosed()) {
					AtomicInteger num = connectionReferences.get(connection);
					if (num != null && num.get() > 0) {
						boolean transactionAware = requestWrapper.isTransactionAware();
						try {
							if (transactionAware) {
								connection.close();
							} else {
								final DataSource dataSource = requestWrapper.getOriginalRequest().getDataSource();
								DataSourceUtils.doReleaseConnection(connection, dataSource);
							}
						} catch (Throwable ex) {
							logger.info("Could not close JDBC Connection", ex);
						}
					}
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.info("Could not close JDBC Connection", e);
			}
		}
	}
}
