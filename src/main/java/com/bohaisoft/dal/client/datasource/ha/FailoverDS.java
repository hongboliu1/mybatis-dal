package com.bohaisoft.dal.client.datasource.ha;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;

public class FailoverDS implements InitializingBean {

	protected String detectingSql;
	protected long monitorPeriod = 20 * 1000;
	protected int initialDelay = 0;
	protected long detectingTimeoutThreshold = 15 * 1000;
	protected long recheckInterval = 3 * 1000;
	protected int recheckTimes = 3;
	protected boolean passiveFailoverEnable = false;
	protected boolean positiveFailoverEnable = false;

	public boolean isPassiveFailoverEnable() {
		return passiveFailoverEnable;
	}

	public void setPassiveFailoverEnable(boolean passiveFailoverEnable) {
		this.passiveFailoverEnable = passiveFailoverEnable;
	}

	public boolean isPositiveFailoverEnable() {
		return positiveFailoverEnable;
	}

	public void setPositiveFailoverEnable(boolean positiveFailoverEnable) {
		this.positiveFailoverEnable = positiveFailoverEnable;
	}

	public long getMonitorPeriod() {
		return monitorPeriod;
	}

	public void setMonitorPeriod(long monitorPeriod) {
		this.monitorPeriod = monitorPeriod;
	}

	public int getInitialDelay() {
		return initialDelay;
	}

	public void setInitialDelay(int initialDelay) {
		this.initialDelay = initialDelay;
	}

	public long getDetectingTimeoutThreshold() {
		return detectingTimeoutThreshold;
	}

	public void setDetectingTimeoutThreshold(long detectingTimeoutThreshold) {
		this.detectingTimeoutThreshold = detectingTimeoutThreshold;
	}

	public long getRecheckInterval() {
		return recheckInterval;
	}

	public void setRecheckInterval(long recheckInterval) {
		this.recheckInterval = recheckInterval;
	}

	public int getRecheckTimes() {
		return recheckTimes;
	}

	public void setRecheckTimes(int recheckTimes) {
		this.recheckTimes = recheckTimes;
	}

	public String getDetectingSql() {
		return detectingSql;
	}

	public void setDetectingSql(String detectingSql) {
		this.detectingSql = detectingSql;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (!isPassiveFailoverEnable() && !isPositiveFailoverEnable()) {
			return;
		}

		if (monitorPeriod <= 0 || detectingTimeoutThreshold <= 0 || recheckInterval <= 0 || recheckTimes <= 0) {
			throw new IllegalArgumentException("'monitorPeriod' OR 'detectingTimeoutThreshold' OR 'recheckInterval' OR 'recheckTimes' must be positive.");
		}

		if (isPositiveFailoverEnable()) {
			if (StringUtils.isEmpty(detectingSql)) {
				throw new IllegalArgumentException("A 'detectingSql' should be provided if positive failover function is enabled.");
			}

			if ((detectingTimeoutThreshold > monitorPeriod)) {
				throw new IllegalArgumentException("the 'detectingTimeoutThreshold' should be less(or equals) than 'monitorPeriod'.");
			}

			if ((recheckInterval * recheckTimes) > detectingTimeoutThreshold) {
				throw new IllegalArgumentException(" 'recheckInterval * recheckTimes' can not be longer than 'detectingTimeoutThreshold'");
			}
		}
		
		if(isPassiveFailoverEnable()) {
			if (StringUtils.isEmpty(detectingSql)) {
				throw new IllegalArgumentException("A 'detectingSql' should be provided if passive failover function is enabled.");
			}
		}
	}
}
