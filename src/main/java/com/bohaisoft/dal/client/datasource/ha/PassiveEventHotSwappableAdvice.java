package com.bohaisoft.dal.client.datasource.ha;

import java.sql.SQLException;

import javax.sql.DataSource;

import com.bohaisoft.dal.client.datasource.GroupDS;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.target.HotSwappableTargetSource;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.support.SQLStateSQLExceptionTranslator;


public class PassiveEventHotSwappableAdvice implements MethodInterceptor, InitializingBean {
	static final Logger logger = LoggerFactory.getLogger(PassiveEventHotSwappableAdvice.class);

	private SQLStateSQLExceptionTranslator sqlExTranslator = new ExtSQLStateSQLExceptionTranslator();

	/**
	 * threshold to indicate until how many times we will stop hot swap between
	 * HA data sources.<br>
	 * default behavior is always swap(with threshold value to be
	 * Integer.MAX_VALUE).
	 */
	private Integer swapTimesThreshold = Integer.MAX_VALUE;

	private GroupDS groupDS;
	private HotSwappableTargetSource targetSource;
	private IFailoverHandler failoverHandler;
	private DataSource masterDataSource;
	private DataSource standbyDataSource;

	Integer lastResult = 0;

	public DataSource getMasterDataSource() {
		return masterDataSource;
	}

	public void setMasterDataSource(DataSource masterDataSource) {
		this.masterDataSource = masterDataSource;
	}

	public DataSource getStandbyDataSource() {
		return standbyDataSource;
	}

	public void setStandbyDataSource(DataSource standbyDataSource) {
		this.standbyDataSource = standbyDataSource;
	}

	/**
	 * @return the targetSource
	 */
	public HotSwappableTargetSource getTargetSource() {
		return targetSource;
	}

	/**
	 * @param targetSource
	 *            the targetSource to set
	 */
	public void setTargetSource(HotSwappableTargetSource targetSource) {
		this.targetSource = targetSource;
	}

	public IFailoverHandler getFailoverHandler() {
		return failoverHandler;
	}

	public void setFailoverHandler(IFailoverHandler failoverHandler) {
		this.failoverHandler = failoverHandler;
	}

	public GroupDS getGroupDS() {
		return groupDS;
	}

	public void setGroupDS(GroupDS groupDS) {
		this.groupDS = groupDS;
	}

	public Object invoke(MethodInvocation invocation) throws Throwable {
		if (!StringUtils.equalsIgnoreCase(invocation.getMethod().getName(), "getConnection")) {
			return invocation.proceed();
		}

		try {
			Object result = invocation.proceed();
			// need to check with detecting sql?
			if ((invocation.getThis() instanceof DataSource) && groupDS.isDisabledSlave((DataSource) invocation.getThis())) {
				groupDS.enableSlave((DataSource) invocation.getThis());
			}
			return result;
		} catch (Throwable t) {
			if (t instanceof SQLException) {
				// we use SQLStateSQLExceptionTranslator to translate
				// SQLExceptions , but it doesn't mean it will work as we
				// expected,
				// so maybe more scope should be covered. we will check out
				// later with runtime data statistics.
				DataAccessException dae = sqlExTranslator.translate("translate to check whether it's a resource failure exception", null, (SQLException) t);
				if (dae instanceof DataAccessResourceFailureException) {
					logger.warn("failed to get Connection from data source with exception:\n{}", t);
					logger.warn("switch to standby data source......");
					DataSource standby = null;
					synchronized (groupDS) {
						standby = failoverHandler.handleUnavailable(groupDS, targetSource, masterDataSource, standbyDataSource);
					}
					logger.warn("available standby data source[" + standby + "].");
					return invocation.getMethod().invoke(standby, invocation.getArguments());
				}
			}
			// other exception conditions should be handled by application,
			// 'cause we don't have enough context information to decide what to
			// do here.
			throw t;
		}
	}

	public Integer getSwapTimesThreshold() {
		return swapTimesThreshold;
	}

	public void setSwapTimesThreshold(Integer swapTimesThreshold) {
		this.swapTimesThreshold = swapTimesThreshold;
	}

	public void afterPropertiesSet() throws Exception {
		if (groupDS == null || failoverHandler == null || targetSource == null) {
			throw new IllegalArgumentException("the groupDS、failoverHandler、targetSource must be set.");
		}
	}

}
