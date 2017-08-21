/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router;

import com.bohaisoft.dal.client.router.config.dataobject.ShardingFactDO;
import com.bohaisoft.dal.client.router.config.dataobject.ShardingMapping;

import java.util.List;
import java.util.Map;



/**
 * 
 * 分库分表服务接口
 * @author wuxiang
 * @since 2012-3-13
 */
public interface ShardingFacade {
	
	/**
	 * 根据分片参数获取目标数据源组
	 * @param shardingFact 分片参数
	 * @return 目标数据源组名称数组
	 */
	public Map<String, List<ShardingMapping>> decideTargetDatasourceGroups(ShardingFactDO shardingFact);
	
	/**
	 * 根据分片参数获取虚拟表名称与实际操作的目标表名映射
	 * @param shardingFact
	 * @return 目标表名称数组
	 */
	public List<ShardingMapping> decideTargetTableNames(ShardingFactDO shardingFact);
}
