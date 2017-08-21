package com.bohaisoft.dal.client.aggregate;

/** 
 * SUM聚合函数支持接口
 * @author meiwei
 * @date 2014-1-24 下午03:41:34
 */
public interface ISupportSum {
	/**
	 * 求和
	 * @author meiwei
	 * @date 2014-1-26 上午09:59:34
	 * @param base
	 * @param x void
	 */
	public void sum(Object base, Object x);
}
