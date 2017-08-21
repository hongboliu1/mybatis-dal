package com.bohaisoft.dal.client.router;

import com.bohaisoft.dal.client.datasource.AtomDS;
import com.bohaisoft.dal.client.datasource.GroupDS;
import com.bohaisoft.dal.client.datasource.MatrixDS;
import com.bohaisoft.dal.client.router.config.dataobject.ShardingFactDO;
import com.bohaisoft.dal.client.router.config.dataobject.ShardingMapping;

import java.util.List;
import java.util.Map;




/**
 * 获得Atom数据源
 * 
 * @author duxinyun
 * 
 */
public interface RoutingFacade {
	
	/**
	 *  
	 * @param shardingFact
	 * @return 原子数据源
	 */
	public Map<AtomDS, List<ShardingMapping>> getAtomDataSources(ShardingFactDO shardingFact);
	
	/**
	 * 
	 * @param groupDataSources
	 * @param shardingFact
	 * @return slave原子数据源
	 */
	public Map<AtomDS, List<ShardingMapping>> getSlaveDataSources(Map<GroupDS, List<ShardingMapping>> groupDataSources, ShardingFactDO shardingFact);
	
	/**
	 * @param shardingFact
	 * @return 返回具体操作的数据源组
	 */
	public Map<GroupDS, List<ShardingMapping>> getGroupDataSources(ShardingFactDO shardingFact);
	
	/**
	 * 
	 * @param shardingFact
	 * @return 返回虚拟表名称与具体操作的实际分表名称的映射
	 */
	public List<ShardingMapping> getVirtualTablesMapping(ShardingFactDO shardingFact);
	
	public MatrixDS getMatrixDS();
}
