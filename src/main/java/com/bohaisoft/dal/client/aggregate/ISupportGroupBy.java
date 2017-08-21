package com.bohaisoft.dal.client.aggregate;

/** 
 * 分组支持
 * @author meiwei
 * @date 2014-1-24 下午03:51:50
 */
public interface ISupportGroupBy {
	/**
	 * 分组的属性
	 * @author meiwei
	 * @date 2014-1-24 下午03:52:15
	 * @param groupBy
	 * @return Object
	 */
	public Object getGroupBy(Object groupBy);
	
	/**
	 * 分组处理
	 * @author meiwei
	 * @date 2014-1-24 下午03:52:34
	 * @param base
	 * @param x void
	 */
	public void group(Object base, Object x);
}
