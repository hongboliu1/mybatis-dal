package com.bohaisoft.dal.client.datasource;

import com.bohaisoft.dal.client.datasource.ha.FailoverDS;
import org.apache.commons.lang3.StringUtils;
import org.springframework.aop.target.HotSwappableTargetSource;
import org.springframework.beans.factory.InitializingBean;


public class MasterDS extends FailoverDS implements InitializingBean {

	private String name;
	private AtomDS master;
	private AtomDS hotBackup;
	private AtomDS masterDetector;// data source for master db status checking
	private AtomDS hotBackupDetector;// data source for hot backup db status
										// checking
	private AtomDS currentDetector;
	private HotSwappableTargetSource targetSource;

	public HotSwappableTargetSource getTargetSource() {
		return targetSource;
	}

	public void setTargetSource(HotSwappableTargetSource targetSource) {
		this.targetSource = targetSource;
	}

	public AtomDS getCurrentDetector() {
		return currentDetector;
	}

	public void setCurrentDetector(AtomDS currentDetector) {
		this.currentDetector = currentDetector;
	}

	/**
	 * @return the masterDetector
	 */
	public AtomDS getMasterDetector() {
		return masterDetector;
	}

	/**
	 * @param masterDetector
	 *            the masterDetector to set
	 */
	public void setMasterDetector(AtomDS masterDetector) {
		this.masterDetector = masterDetector;
	}

	/**
	 * @return the hotBackupDetector
	 */
	public AtomDS getHotBackupDetector() {
		return hotBackupDetector;
	}

	/**
	 * @param hotBackupDetector
	 *            the hotBackupDetector to set
	 */
	public void setHotBackupDetector(AtomDS hotBackupDetector) {
		this.hotBackupDetector = hotBackupDetector;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public AtomDS getMaster() {
		return master;
	}

	public void setMaster(AtomDS master) {
		this.master = master;
	}

	public AtomDS getHotBackup() {
		return hotBackup;
	}

	public void setHotBackup(AtomDS hotBackup) {
		this.hotBackup = hotBackup;
	}

	@Override
	public void afterPropertiesSet() {
		if (StringUtils.isEmpty(this.name)) {
			throw new IllegalArgumentException("the 'name' property of MasterDS must be set");
		}
		if(this.master != null) {
			this.master.setParentDS(this);
		}
		if(this.hotBackup != null) {
			this.hotBackup.setParentDS(this);
		}
	}

	@Override
	public String toString() {
		return this.getName();
	}
}
