/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router.rule;

import com.bohaisoft.dal.client.exception.RoutingException;

/**
 * 分片规则
 *	
 * @author wuxiang
 * @since 2012-3-9
 */
public interface ShardingRule {

	/**
	 * 根据分片参数判断得到目标分片名称
	 * @param parameterObject 参数
	 * @return 目标分片名称，对于分表就是目标表名称列表，对于分库就是目标数据源组名称列表
	 * @throws RoutingException
	 */
	public String decideTargetShards(Object parameterObject) throws RoutingException;
}
