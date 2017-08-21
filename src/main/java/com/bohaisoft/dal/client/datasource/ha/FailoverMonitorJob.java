package com.bohaisoft.dal.client.datasource.ha;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.sql.DataSource;

import com.bohaisoft.dal.client.datasource.AtomDS;
import com.bohaisoft.dal.client.datasource.GroupDS;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.target.HotSwappableTargetSource;


public class FailoverMonitorJob implements Runnable {

	static final Logger logger = LoggerFactory.getLogger(FailoverMonitorJob.class);

	private String detectingSQL;
	private boolean detectingWithQuery = false;
	
	/**
	 * time unit in milliseconds
	 */
	private long detectingRequestTimeout;

	private long recheckInterval;
	private int recheckTimes;

	protected GroupDS groupDS;
	protected HotSwappableTargetSource targetSource;
	protected AtomDS currentDetectorTarget;
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

	/**
	 * @param currentDetectorTarget
	 *            the currentDetectorDataSource to set
	 */
	public void setCurrentDetectorTarget(AtomDS currentDetectorTarget) {
		this.currentDetectorTarget = currentDetectorTarget;
	}

	/**
	 * Since {@link FailoverMonitorJob} will be scheduled to run in sequence,
	 * One executor as instance field is ok.<br>
	 * This executor will be used to execute detecting logic asynchronously, if
	 * the execution exceeds given timeout threshold, we will check again before
	 * switching to standby data source.
	 */
	private ExecutorService executor;

	public GroupDS getGroupDS() {
		return groupDS;
	}

	public void setGroupDS(GroupDS groupDS) {
		this.groupDS = groupDS;
	}

	private IFailoverHandler failoverHandler;

	public IFailoverHandler getFailoverHandler() {
		return failoverHandler;
	}

	public void setFailoverHandler(IFailoverHandler failoverHandler) {
		this.failoverHandler = failoverHandler;
	}

	public FailoverMonitorJob(ExecutorService es) {
		Validate.notNull(es);
		this.executor = es;
	}

	public void run() {
		if(!groupDS.isInitialized()) {
			return;
		}
		if(logger.isDebugEnabled()) {
			logger.debug("checking connection of data source[" + targetSource + "], failoverHandler-" + failoverHandler.getClass().getSimpleName());
		}
		Future<Integer> future = executor.submit(new Callable<Integer>() {

			public Integer call() throws Exception {
				Integer result = -1;
				
				for (int i = 0; i < getRecheckTimes(); i++) {
					Connection conn = null;
					try {
						DataSource detector = getCurrentDetectorDataSource();
						if(detector == null) {
							throw new IllegalArgumentException("current data source detector not found");
						}
						conn = detector.getConnection();
						PreparedStatement pstmt = conn.prepareStatement(getDetectingSQL());
						boolean ret = false;
						if(detectingWithQuery) {
							ResultSet rs = pstmt.executeQuery();
							ret = rs.next();
							rs.close();
						} else {
							ret = pstmt.execute();
						}
						if (pstmt != null) {
							pstmt.close();
						}
						if (ret) {
							result = 0;
						}
						break;
					} catch (Exception e) {
						logger.warn("(" + (i + 1) + ") check with failure. sleep (" + getRecheckInterval() + ") for next round check, failoverHandler-" + failoverHandler.getClass().getSimpleName(), e);
						try {
							TimeUnit.MILLISECONDS.sleep(getRecheckInterval());
						} catch (InterruptedException e1) {
							logger.warn("interrupted when waiting for next round rechecking.");
						}
						continue;
					} finally {
						if (conn != null) {
							try {
								conn.close();
							} catch (SQLException e) {
								logger.warn("failed to close checking connection:\n", e);
							}
						}
					}
				}
				return result;
			}
		});

		try {
			Integer result = future.get(getDetectingRequestTimeout(), TimeUnit.MILLISECONDS);
			if (!result.equals(lastResult)) {
				if (result == -1) {
					logger.warn("the data source[" + targetSource + "] is unavailable,switch to standby data source, failoverHandler-" + failoverHandler.getClass().getSimpleName());
					synchronized(groupDS) {
						failoverHandler.handleUnavailable(groupDS, targetSource, masterDataSource, standbyDataSource);
					}
				} else {
					logger.warn("the data source[" + targetSource + "] is available, reuse the data source.");
					synchronized(groupDS) {
						failoverHandler.handleAvailable(groupDS, targetSource);
					}
				}
			}
			lastResult = result;
		} catch (InterruptedException e) {
			logger.warn("interrupted when getting query result in FailoverMonitorJob.");
			lastResult = -1;
		} catch (ExecutionException e) {
			logger.warn("exception occured when checking failover status in FailoverMonitorJob.");
			lastResult = -1;
		} catch (TimeoutException e) {
			logger.warn("exceed DetectingRequestTimeout threshold. Switch to standby data source, failoverHandler-" + failoverHandler.getClass().getSimpleName());
			synchronized(groupDS) {
				failoverHandler.handleUnavailable(groupDS, targetSource, masterDataSource, standbyDataSource);
			}
			lastResult = -1;
		}
	}

	public String getDetectingSQL() {
		return detectingSQL;
	}

	public void setDetectingSQL(String detectingSQL) {
		this.detectingSQL = detectingSQL;
		if(detectingSQL.trim().toLowerCase().startsWith("select ")) {
			detectingWithQuery = true;
		}
	}

	public long getDetectingRequestTimeout() {
		return detectingRequestTimeout;
	}

	public void setDetectingRequestTimeout(long detectingRequestTimeout) {
		this.detectingRequestTimeout = detectingRequestTimeout;
	}

	public void setRecheckInterval(long recheckInterval) {
		this.recheckInterval = recheckInterval;
	}

	public long getRecheckInterval() {
		return recheckInterval;
	}

	public void setRecheckTimes(int recheckTimes) {
		this.recheckTimes = recheckTimes;
	}

	public int getRecheckTimes() {
		return recheckTimes;
	}

	public DataSource getCurrentDetectorDataSource() {
		AtomDS detector = failoverHandler.determineCurrentDetectorDataSource(groupDS, currentDetectorTarget);
		if(detector != null) {
			return detector.getOriginalDataSource();
		} else {
			return null;
		}
	}

}
