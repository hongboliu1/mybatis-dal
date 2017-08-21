package com.bohaisoft.dal.client.aggregate;

import java.util.List;

/** 
 * 聚合接口
 * @author meiwei
 * @date 2014-1-24 下午03:36:20
 */
public interface IAggregate {
	/**
	 * 聚合回调
	 * @author meiwei
	 * @date 2014-1-24 下午03:40:18
	 * @param list
	 * @return Object
	 */
	public Object execute(List<Object> list, Integer dsSize);
	public Object execute(List<Object> list);

	
	
}
