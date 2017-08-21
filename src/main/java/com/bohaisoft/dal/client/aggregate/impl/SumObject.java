package com.bohaisoft.dal.client.aggregate.impl;

import java.util.List;

import com.bohaisoft.dal.client.aggregate.IAggregate;
import com.bohaisoft.dal.client.aggregate.ISupportSum;
import org.springframework.util.CollectionUtils;



/** 
 * 对象求和
 * @author meiwei
 * @date 2014-1-26 下午01:39:32
 */
public class SumObject implements IAggregate {
	
	private ISupportSum supportSum;

	public SumObject(ISupportSum supportSum) {
		this.supportSum = supportSum;
	}
	
	@Override
	public Object execute(List<Object> list,Integer dsSize){
		return execute(list);
	}
	
	@Override
	public Object execute(List<Object> list) {
		if (null == supportSum || CollectionUtils.isEmpty(list)) {
			return list;
		}
		// 如果只有一个元素，直接返回
		if (list.size() == 1) {
			return list.get(0);
		}
		// 多个元素，做聚合
		Object base = list.get(0);
		for (int i = 1; i < list.size(); i++) {
			supportSum.sum(base, list.get(i));
		}
		return base;
	}

}
