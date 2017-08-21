package com.bohaisoft.dal.client.router.config.dataobject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

public class SqlConfigDO implements InitializingBean {
	private final static Map<String, Boolean> isMasterMap = new HashMap<String, Boolean>();
	static final Logger logger = LoggerFactory.getLogger(SqlConfigDO.class);
	// 只操作主库的sql集合
	private Set<String> masterSqlId = new HashSet<String>();
	// 只操作备库的sql集合
	private Set<String> slaveSqlId = new HashSet<String>();
	// sql对应的数据源配置
	private Map<String, DataSourceConfig> sqlDataSourceMapping = new HashMap<String, DataSourceConfig>();

	public void setSqlDataSourceMapping(Map<String, DataSourceConfig> sqlDataSourceMapping) {
		this.sqlDataSourceMapping = sqlDataSourceMapping;
	}

	public Set<String> getMasterSqlId() {
		return masterSqlId;
	}

	public void setMasterSqlId(Set<String> masterSqlId) {
		this.masterSqlId = masterSqlId;
	}

	public Set<String> getSlaveSqlId() {
		return slaveSqlId;
	}

	public void setSlaveSqlId(Set<String> slaveSqlId) {
		this.slaveSqlId = slaveSqlId;
	}

	/**
	 * 解析spring-sqlId-IsMaster.xml 根据sqlId 可以获得是否 只操作主库 或者 只操作备库
	 * 
	 * @param sqlId
	 * @return 如果 没有找到该sqlId 对应的信息 返回null 如果 返回true操作主库，false 操作备库
	 */
	public Boolean isMaster(String sqlId) {
		if (isMasterMap.containsKey(sqlId)) {
			return isMasterMap.get(sqlId);
		}
		return null;
	}

	public DataSourceConfig getDataSourceConfig(String sqlId) {
		return sqlDataSourceMapping.get(sqlId);
	}

	// 如果解析时发现 同一条sqlId 同时存在主库和备库集合中 则以该sqlId 操作主库为准
	@Override
	public void afterPropertiesSet() throws Exception {
		// 组拼map集合
		if (!masterSqlId.isEmpty()) {
			Iterator<String> masterIterSQLIter = masterSqlId.iterator();
			while (masterIterSQLIter.hasNext()) {
				String masterSQLId = masterIterSQLIter.next();
				isMasterMap.put(masterSQLId, true);
			}
		}
		if (!slaveSqlId.isEmpty()) {
			Iterator<String> slaveSQLIter = slaveSqlId.iterator();
			while (slaveSQLIter.hasNext()) {
				String slaveSQLId = slaveSQLIter.next();
				if (masterSqlId.contains(slaveSQLId)) {
					logger.warn("Duplicated definition in 'slaveSqlId' set of 'dal-sqlconfig' file with sql id:" + slaveSQLId);
				} else {
					isMasterMap.put(slaveSQLId, false);
				}

			}
		}
		if (!sqlDataSourceMapping.isEmpty()) {// 保持兼容，sqlIdDataSource里的isMaster优先级高于masterSqlId、slaveSqlId
			for (String sqlId : sqlDataSourceMapping.keySet()) {
				DataSourceConfig config = sqlDataSourceMapping.get(sqlId);
				if (config != null) {
					if (config.getIsMaster() != null) {
						isMasterMap.put(sqlId, config.getIsMaster());
					}
					if (config.getSlaveDS() != null && config.getGroupDS() == null) {
						throw new IllegalArgumentException("The 'groupDS' property of 'sqlIdDatasource' in 'dal-sqlconfig' xml is empty");
					}
				}

			}
		}
	}

}
