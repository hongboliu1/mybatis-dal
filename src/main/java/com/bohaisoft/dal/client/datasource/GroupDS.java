package com.bohaisoft.dal.client.datasource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.bohaisoft.dal.client.datasource.ha.*;
import com.bohaisoft.dal.client.datasource.reload.DefaultDataSourceLoader;
import com.bohaisoft.dal.client.datasource.reload.IDataSourceLoader;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.target.HotSwappableTargetSource;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;


/**
 * 每个组 有主库 备库 备库有权重之分
 * 
 * @author duxinyun
 * 
 */
public class GroupDS implements InitializingBean, DisposableBean {

	static final Logger logger = LoggerFactory.getLogger(GroupDS.class);

	private String name;
	private String description;
	private MasterDS master;
	private Map<String, SlaveDS> slave;
	private Map<String, SlaveDS> disabledSlaves = new HashMap<String, SlaveDS>();
	private Map<String, Integer> mapToWeight = new HashMap<String, Integer>();
	private Map<String, AtomDS> mapToSlaveDS = new HashMap<String, AtomDS>();
	private boolean isInitialized = false;
	private Boolean isDefaultMaster;//默认操作主库还是备库
	private ConcurrentMap<ScheduledFuture<?>, ScheduledExecutorService> schedulerFutures = new ConcurrentHashMap<ScheduledFuture<?>, ScheduledExecutorService>();
	private List<ExecutorService> jobExecutorRegistry = new ArrayList<ExecutorService>();

	private boolean reloadDataSourceWhileFailure = false;//某个数据源不可用时是否重启该数据源
	private boolean slaveFailoverToMaster = true;//备库不可用是否转移到主库
	private boolean testTargetDataSourceBeforeFailover = true;//当故障转移切换时测试目标数据源是否可用
	
	public boolean isTestTargetDataSourceBeforeFailover() {
		return testTargetDataSourceBeforeFailover;
	}

	public void setTestTargetDataSourceBeforeFailover(boolean testTargetDataSourceBeforeFailover) {
		this.testTargetDataSourceBeforeFailover = testTargetDataSourceBeforeFailover;
	}

	private IDataSourceLoader dataSourceLoader = DefaultDataSourceLoader.INSTANCE;//用于数据源reload
	
	public IDataSourceLoader getDataSourceLoader() {
		return dataSourceLoader;
	}

	public void setDataSourceLoader(IDataSourceLoader dataSourceLoader) {
		this.dataSourceLoader = dataSourceLoader;
	}

	public boolean isReloadDataSourceWhileFailure() {
		return reloadDataSourceWhileFailure;
	}

	public void setReloadDataSourceWhileFailure(boolean reloadDataSourceWhileFailure) {
		this.reloadDataSourceWhileFailure = reloadDataSourceWhileFailure;
	}

	public boolean isSlaveFailoverToMaster() {
		return slaveFailoverToMaster;
	}

	public void setSlaveFailoverToMaster(boolean slaveFailoverToMaster) {
		this.slaveFailoverToMaster = slaveFailoverToMaster;
	}

	public Boolean isDefaultMaster() {
		return isDefaultMaster;
	}

	public void setIsDefaultMaster(Boolean isDefaultMaster) {
		this.isDefaultMaster = isDefaultMaster;
	}

	/**
	 * @return the isInitialized
	 */
	public boolean isInitialized() {
		return isInitialized;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	/** 返回 master的数据源 */
	public AtomDS getMasterDS() {
		return master.getMaster();
	}

	public MasterDS getMaster() {
		return master;
	}

	public void setMaster(MasterDS master) {
		this.master = master;
	}

	public Map<String, SlaveDS> getSlave() {
		return slave;
	}

	public void setSlave(Map<String, SlaveDS> slave) {
		this.slave = slave;
	}

	/** key：slaveDS value：权重 */
	public Map<String, Integer> getMapToWeight() {
		return mapToWeight;
	}

	public void setMapToWeight(Map<String, Integer> mapToWeight) {
		this.mapToWeight = mapToWeight;
	}

	/** key：slaveDS value：AtomDS */
	public Map<String, AtomDS> getMapToSlaveDS() {
		return mapToSlaveDS;
	}

	public void setMapToSlaveDS(Map<String, AtomDS> mapToSlaveDS) {
		this.mapToSlaveDS = mapToSlaveDS;
	}

	public void disableSlave(DataSource ds) {
		if (ds == null) {
			return;
		}
		if (this.slave != null) {
			Map<String, AtomDS> mapToSlaveDS2 = new HashMap<String, AtomDS>();
			Map<String, Integer> mapToWeight2 = new HashMap<String, Integer>();
			synchronized (mapToSlaveDS) {
				String dsKey = null;
				for (String key : slave.keySet()) {
					SlaveDS slaveDS = slave.get(key);
					if (slaveDS == null || slaveDS.getDataSource() == null) {
						continue;
					}
					if (ds == slaveDS.getDataSource().getOriginalDataSource()) {
						slaveDS.getDataSource().setEnabled(false);
						disabledSlaves.put(key, slaveDS);
						dsKey = key;
						continue;
					}
					mapToSlaveDS2.put(key, slaveDS.getDataSource());
					mapToWeight2.put(key, slaveDS.getWeight());
					// ds.setTargetDataSource(targetDataSource);
				}
				if (dsKey != null) {
					mapToSlaveDS = mapToSlaveDS2;
					mapToWeight = mapToWeight2;
				}
			}
		}
	}

	public SlaveDS getDisabledSlave(String slaveName) {
		return this.disabledSlaves.get(slaveName);
	}

	public boolean isDisabledSlave(DataSource ds) {
		if (this.slave != null && !disabledSlaves.isEmpty()) {
			synchronized (mapToSlaveDS) {
				for (String key : disabledSlaves.keySet()) {
					SlaveDS slaveDS = slave.get(key);
					if (ds == slaveDS.getDataSource().getOriginalDataSource()) {
						return true;
					}
				}
			}
		}
		return false;
	}

	public void enableSlave(DataSource ds) {
		if (ds == null) {
			return;
		}
		if (this.slave != null && !disabledSlaves.isEmpty()) {
			Map<String, AtomDS> mapToSlaveDS2 = new HashMap<String, AtomDS>();
			Map<String, Integer> mapToWeight2 = new HashMap<String, Integer>();
			synchronized (mapToSlaveDS) {
				mapToSlaveDS2.putAll(mapToSlaveDS);
				mapToWeight2.putAll(mapToWeight);

				String dsKey = null;
				for (String key : disabledSlaves.keySet()) {
					SlaveDS slaveDS = slave.get(key);
					if (ds == slaveDS.getDataSource().getOriginalDataSource()) {
						slaveDS.getDataSource().setEnabled(true);
						mapToSlaveDS2.put(key, slaveDS.getDataSource());
						mapToWeight2.put(key, slaveDS.getWeight());
						dsKey = key;
					}
				}
				if (dsKey != null) {
					mapToSlaveDS = mapToSlaveDS2;
					mapToWeight = mapToWeight2;
					disabledSlaves.remove(dsKey);
				}
			}
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (StringUtils.isEmpty(this.name)) {
			throw new IllegalArgumentException("the 'name' property of GroupDS must be set");
		}
		if(dataSourceLoader == null) {
			throw new IllegalArgumentException("the 'dataSourceLoader' property is required");
		}
		if (this.slave != null) {
			for (String key : slave.keySet()) {
				SlaveDS slaveDS = slave.get(key);
				if (slaveDS == null || slaveDS.getDataSource() == null) {
					continue;
				}
				AtomDS ds = slaveDS.getDataSource();
				mapToSlaveDS.put(key, ds);
				mapToWeight.put(key, slaveDS.getWeight());
				if (ds.getGroupDS() != null && ds.getGroupDS() != this) {
					throw new IllegalArgumentException("the atom data source '" + ds + "' was already defined in the data source group '" + ds.getGroupDS() + "'");
				}
				ds.setGroupDS(this);
				ds.setTargetDataSource(createSlaveHADataSource(this, slaveDS));
			}
		}
		if (this.master != null) {
			if (master.getMaster() != null) {
				AtomDS ds = master.getMaster();
				if (ds.getGroupDS() != null && ds.getGroupDS() != this) {
					throw new IllegalArgumentException("the atom data source '" + ds + "' was already defined in the data source group '" + ds.getGroupDS() + "'");
				}
				master.getMaster().setGroupDS(this);
				master.getMaster().setTargetDataSource(createMasterHADataSource(this));
			}
			if (master.getHotBackup() != null) {
				AtomDS ds = master.getHotBackup();
				if (ds.getGroupDS() != null && ds.getGroupDS() != this) {
					throw new IllegalArgumentException("the atom data source '" + ds + "' was already defined in the data source group '" + ds.getGroupDS() + "'");
				}
				master.getHotBackup().setGroupDS(this);
				if (master.getMaster() == null) {
					master.getHotBackup().setTargetDataSource(createMasterHADataSource(this));
				}
			}
		}
		this.isInitialized = true;
	}

	@Override
	public String toString() {
		return this.getName() + (this.getDescription() != null ? ("-" + this.getDescription()) : "");
	}

	public DataSource createMasterHADataSource(GroupDS groupDS) throws Exception {
		if (groupDS == null) {
			throw new IllegalArgumentException("groupDS can't be null.");
		}
		MasterDS masterDS = groupDS.getMaster();
		if (masterDS == null) {
			throw new IllegalArgumentException("masterDS can't be null.");
		}
		AtomDS masterDataSource = masterDS.getMaster();
		AtomDS standbyDataSource = masterDS.getHotBackup();
		if (masterDataSource == null && standbyDataSource == null) {
			throw new IllegalArgumentException("must have at least one data source active.");
		}
		if (!masterDS.isPassiveFailoverEnable() && !masterDS.isPositiveFailoverEnable()) {
			if (masterDataSource == null) {
				return standbyDataSource.getTargetDataSource();
			}
			return masterDataSource.getTargetDataSource();
		}
		if(masterDS.getMasterDetector() == null) {
			masterDS.setMasterDetector(masterDataSource);
		}
		if(masterDS.getHotBackupDetector() == null) {
			masterDS.setHotBackupDetector(standbyDataSource);
		}
		masterDS.setCurrentDetector(masterDS.getMasterDetector());
		return createHADataSource(groupDS, masterDS, masterDataSource, standbyDataSource, masterDS.getCurrentDetector(), new MasterFailoverHandler());
	}

	public DataSource createSlaveHADataSource(GroupDS groupDS, SlaveDS slaveDS) throws Exception {
		if (groupDS == null) {
			throw new IllegalArgumentException("groupDS can't be null.");
		}
		if (slaveDS == null) {
			throw new IllegalArgumentException("slaveDS can't be null.");
		}
		if(slaveDS.getSlaveDetector() == null) {
			slaveDS.setSlaveDetector(slaveDS.getDataSource());
		}
		return createHADataSource(groupDS, slaveDS, slaveDS.getDataSource(), null, slaveDS.getSlaveDetector(), new SlaveFailoverHandler());
	}

	public DataSource createHADataSource(GroupDS groupDS, FailoverDS ds, AtomDS masterDataSource, AtomDS standbyDataSource, AtomDS detectorDataSource, IFailoverHandler failoverHandler)
			throws Exception {
		if (groupDS == null || ds == null) {
			throw new IllegalArgumentException("data source can't be null.");
		}

		if (masterDataSource == null) {
			throw new IllegalArgumentException("must have an active data source.");
		}

		HotSwappableTargetSource targetSource = new HotSwappableTargetSource(masterDataSource.getOriginalDataSource());
		ProxyFactory pf = new ProxyFactory();
		pf.setInterfaces(new Class[] { DataSource.class });
		pf.setTargetSource(targetSource);

		if(detectorDataSource == null) {
			detectorDataSource = masterDataSource;
		}
//		HotSwappableTargetSource detectorTarget = null;
//		if (detectorDataSource != null) {
//			detectorTarget = new HotSwappableTargetSource(detectorDataSource.getOriginalDataSource());
//			;
//		} else {
//			detectorTarget = targetSource;
//		}
		if (ds.isPositiveFailoverEnable()) {
			// 1. create active monitoring job for failover event
			ThreadFactory schedulerFactory = new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					Thread t = new Thread(r);
					t.setName("FailoverMonitorJob-scheduler");
					t.setDaemon(true);
					return t;
				}
			};
			ThreadFactory jobFactory = new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					Thread t = new Thread(r);
					t.setName("FailoverMonitorJob-job");
					t.setDaemon(true);
					return t;
				}
			};
			ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, schedulerFactory);
			ExecutorService jobExecutor = Executors.newFixedThreadPool(1, jobFactory);
			jobExecutorRegistry.add(jobExecutor);
			FailoverMonitorJob job = new FailoverMonitorJob(jobExecutor);
			// 1.1 inject dependencies
			job.setGroupDS(groupDS);
			job.setFailoverHandler(failoverHandler);
			job.setDetectingRequestTimeout(ds.getDetectingTimeoutThreshold());
			job.setDetectingSQL(ds.getDetectingSql());
			job.setRecheckInterval(ds.getRecheckInterval());
			job.setRecheckTimes(ds.getRecheckTimes());
			job.setCurrentDetectorTarget(detectorDataSource);
			job.setTargetSource(targetSource);
			job.setMasterDataSource(masterDataSource.getOriginalDataSource());
			if (standbyDataSource != null) {
				job.setStandbyDataSource(standbyDataSource.getOriginalDataSource());
			}
			// 1.2 start scheduling and keep reference for canceling and
			// shutdown
			ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(job, ds.getInitialDelay(), ds.getMonitorPeriod(), TimeUnit.MILLISECONDS);
			schedulerFutures.put(future, scheduler);
		}

		if (ds.isPassiveFailoverEnable()) {
			// 2. create data source proxy with passive event advice
			PassiveEventHotSwappableAdvice advice = new PassiveEventHotSwappableAdvice();
			advice.setGroupDS(groupDS);
			advice.setFailoverHandler(failoverHandler);
			advice.setTargetSource(targetSource);
			advice.setMasterDataSource(masterDataSource.getOriginalDataSource());
			if (standbyDataSource != null) {
				advice.setStandbyDataSource(standbyDataSource.getOriginalDataSource());
			}

			pf.addAdvice(advice);
		}

		return (DataSource) pf.getProxy();
	}

	public void destroy() throws Exception {

		for (Map.Entry<ScheduledFuture<?>, ScheduledExecutorService> e : schedulerFutures.entrySet()) {
			ScheduledFuture<?> future = e.getKey();
			ScheduledExecutorService scheduler = e.getValue();
			future.cancel(true);
			shutdownExecutor(scheduler);
		}

		for (ExecutorService executor : jobExecutorRegistry) {
			shutdownExecutor(executor);
		}
	}

	private void shutdownExecutor(ExecutorService executor) {
		try {
			executor.shutdown();
			executor.awaitTermination(5, TimeUnit.SECONDS);
		} catch (Exception ex) {
			logger.warn("interrupted when shutting down executor service.");
		}
	}

}
