package com.bohaisoft.dal.client.router.impl;

import com.bohaisoft.dal.client.cache.ICache;
import com.bohaisoft.dal.client.datasource.AtomDS;
import com.bohaisoft.dal.client.datasource.GroupDS;
import com.bohaisoft.dal.client.datasource.MatrixDS;
import com.bohaisoft.dal.client.exception.NoDatasourceFoundException;
import com.bohaisoft.dal.client.router.RoutingFacade;
import com.bohaisoft.dal.client.router.ShardingFacade;
import com.bohaisoft.dal.client.router.config.dataobject.DataSourceConfig;
import com.bohaisoft.dal.client.router.config.dataobject.ShardingFactDO;
import com.bohaisoft.dal.client.router.config.dataobject.ShardingMapping;
import com.bohaisoft.dal.client.router.config.dataobject.SqlConfigDO;
import com.bohaisoft.dal.client.router.support.RequestFactCtx;
import com.bohaisoft.dal.client.router.support.RequestFactCtxVO;
import com.bohaisoft.dal.client.support.utils.SlaveDsStrategy;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RoutingFacadeImpl implements RoutingFacade, InitializingBean {

    private MatrixDS matrixDS;
    private SqlConfigDO sqlConfig;
    private ICache routingCache = null;

    public ICache getRoutingCache() {
        return routingCache;
    }

    public void setRoutingCache(ICache routingCache) {
        this.routingCache = routingCache;
    }

    private ShardingFacade shardingService;

    public MatrixDS getMatrixDS() {
        return matrixDS;
    }

    public SqlConfigDO getSqlConfig() {
        return sqlConfig;
    }

    @Override
    public Map<AtomDS, List<ShardingMapping>> getAtomDataSources(ShardingFactDO shardingFact) {
        Map<GroupDS, List<ShardingMapping>> groupDataSources = getGroupDataSources(shardingFact);

        String sqlId = shardingFact.getStatementName();
        Boolean isMasterDS = true;
        if (sqlConfig != null) {
            Boolean isGroupMaster = null;
            for (GroupDS ds : groupDataSources.keySet()) {
                if (ds.isDefaultMaster() != null && ds.isDefaultMaster()) {
                    isGroupMaster = Boolean.TRUE;
                    break;
                }
            }
            isMasterDS = this.isMasterDS(shardingFact.getIsMaster(), sqlConfig.isMaster(sqlId), isGroupMaster, matrixDS.isDefaultMaster());
        }

        Map<AtomDS, List<ShardingMapping>> atomDataSources = null;
        if (!isMasterDS) {
            atomDataSources = getSlaveDataSources(groupDataSources, shardingFact);
            if (!CollectionUtils.isEmpty(atomDataSources)) {
                return atomDataSources;
            }
        }

        atomDataSources = new HashMap<>();
        for (GroupDS group : groupDataSources.keySet()) {
            if (group.getMasterDS() != null) {
                atomDataSources.put(group.getMasterDS(), groupDataSources.get(group));
            }
        }
        return atomDataSources;
    }

    public Map<AtomDS, List<ShardingMapping>> getSlaveDataSources(Map<GroupDS, List<ShardingMapping>> groupDataSources, ShardingFactDO shardingFact) {
        Map<AtomDS, List<ShardingMapping>> atomDataSources = new HashMap<>();
        RequestFactCtxVO requestFact = RequestFactCtx.getRfvoholder();
        for (GroupDS group : groupDataSources.keySet()) {
            String slaveName = null;
            if (sqlConfig != null) {
                DataSourceConfig dsConfig = sqlConfig.getDataSourceConfig(shardingFact.getStatementName());
                if (dsConfig != null && dsConfig.getSlaveDS() != null) {
                    slaveName = dsConfig.getSlaveDS();
                }
            }
            if (slaveName == null && requestFact != null) {
                Map<GroupDS, String> groupSlaves = requestFact.getGroupSlavesMapping();
                if (groupSlaves != null) {
                    slaveName = groupSlaves.get(group);
                }
            }
            if (slaveName == null) {
                slaveName = SlaveDsStrategy.getSlaveId(group.getMapToWeight());
            }
            if (StringUtils.isNotEmpty(slaveName)) {
                AtomDS atomDS = group.getMapToSlaveDS().get(slaveName);
                if (atomDS == null) {
                    slaveName = SlaveDsStrategy.getSlaveId(group.getMapToWeight());
                    atomDS = group.getMapToSlaveDS().get(slaveName);
                }
                if (atomDS != null) {
                    if (requestFact == null) {
                        requestFact = new RequestFactCtxVO();
                        Map<GroupDS, String> groupSlaves = new HashMap<GroupDS, String>();
                        groupSlaves.put(group, slaveName);
                        requestFact.setGroupSlavesMapping(groupSlaves);
                        RequestFactCtx.setRfvoholder(requestFact);
                    } else {
                        if (requestFact.getGroupSlavesMapping() == null) {
                            Map<GroupDS, String> groupSlaves = new HashMap<GroupDS, String>();
                            requestFact.setGroupSlavesMapping(groupSlaves);
                        }
                        requestFact.getGroupSlavesMapping().put(group, slaveName);
                    }
                    atomDataSources.put(atomDS, groupDataSources.get(group));
                } else if (!group.isSlaveFailoverToMaster()) {
                    throw new NoDatasourceFoundException("No available slave datasource found:" + shardingFact);
                }
            } else {
                atomDataSources.put(group.getMasterDS(), groupDataSources.get(group));
            }
        }

        return atomDataSources;
    }

    /***
     * @param shardingFact
     * @return 绗竴姝ユ壘鍒癵roups锛屾牴鎹粍鐨刬d 璋冪敤matrixDS涓殑鏂规硶 鑾峰緱GroupDS
     */
    public Map<GroupDS, List<ShardingMapping>> getGroupDataSources(ShardingFactDO shardingFact) {
        Map<GroupDS, List<ShardingMapping>> dataSources = null;
        if (dataSources != null) {
            return dataSources;
        } else {
            dataSources = new HashMap<GroupDS, List<ShardingMapping>>();
        }

        Map<String, GroupDS> groupMaps = matrixDS.getGroupMap();
        String groupDSName = shardingFact.getGroupDS();

        if (groupDSName == null && sqlConfig != null) {
            DataSourceConfig dsConfig = sqlConfig.getDataSourceConfig(shardingFact.getStatementName());
            if (dsConfig != null && dsConfig.getGroupDS() != null) {
                groupDSName = dsConfig.getGroupDS();
            }
        }
        if (groupDSName == null && shardingService != null) {
            Map<String, List<ShardingMapping>> results = shardingService.decideTargetDatasourceGroups(shardingFact);
            for (String group : results.keySet()) {
                List<ShardingMapping> mapping = results.get(group);
                GroupDS groupDS = groupMaps.get(group);
                if (groupDS != null) {
                    dataSources.put(groupDS, mapping);
                }
            }
        } else if (groupDSName != null) {
            GroupDS groupDS = groupMaps.get(groupDSName);
            if (groupDS != null) {
                String groupDSOfFact = shardingFact.getGroupDS();
                if (groupDSOfFact != null && groupDSOfFact.equals(groupDSName)) {
                    dataSources.put(groupDS, shardingFact.getVirtualTablesMapping());
                } else {
                    dataSources.put(groupDS, null);
                }
            }
        }
        if (dataSources.isEmpty()) {
            GroupDS defaultGroupDS = matrixDS.getDefaultGroupDS();
            if (defaultGroupDS != null) {
                dataSources.put(defaultGroupDS, null);
            } else {
                throw new NoDatasourceFoundException("No default datasource found:" + shardingFact);
            }
        }
        return dataSources;
    }

    /**
     * 鍒ゆ柇鏄惁鏄蛋涓诲簱鏁版嵁婧愶紝鍒ゆ柇浼樺厛绾т粠楂樺埌浣庯細
     * isMasterWithThreadLocal>isMasterWithSqlConfig>isMasterWithGroup
     * >isMasterWithMatrix
     *
     * @param isMasterWithThreadLocal 浠嶵hreadLocal鍒ゆ柇鏄惁涓诲簱
     * @param isMasterWithSqlConfig   浠嶴qlConfig鍒ゆ柇鏄惁涓诲簱
     * @param isMasterWithGroup       浠嶨roup鍒ゆ柇鏄惁涓诲簱
     * @param isMasterWithMatrix      浠嶮atrix鍒ゆ柇鏄惁涓诲簱
     * @return
     */
    private Boolean isMasterDS(Boolean isMasterWithThreadLocal, Boolean isMasterWithSqlConfig, Boolean isMasterWithGroup, Boolean isMasterWithMatrix) {
        if (isMasterWithThreadLocal != null) {
            return isMasterWithThreadLocal;
        } else if (isMasterWithThreadLocal == null && isMasterWithSqlConfig != null) {
            return isMasterWithSqlConfig;
        } else if (isMasterWithThreadLocal == null && isMasterWithSqlConfig == null && isMasterWithGroup != null) {
            return isMasterWithGroup;
        }
        return isMasterWithMatrix;
    }

    public void setSqlConfig(SqlConfigDO sqlConfig) {
        this.sqlConfig = sqlConfig;
    }

    public void setMatrixDS(MatrixDS matrixDS) {
        this.matrixDS = matrixDS;
    }

    public ShardingFacade getShardingService() {
        return shardingService;
    }

    public void setShardingService(ShardingFacade shardingService) {
        this.shardingService = shardingService;
    }

    @Override
    public List<ShardingMapping> getVirtualTablesMapping(ShardingFactDO shardingFact) {
        if (shardingService != null) {
            return shardingService.decideTargetTableNames(shardingFact);
        }
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
    }

}
