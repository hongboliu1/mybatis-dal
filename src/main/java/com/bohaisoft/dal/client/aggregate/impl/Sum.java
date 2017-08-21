package com.bohaisoft.dal.client.aggregate.impl;

import java.util.List;

import com.bohaisoft.dal.client.aggregate.IAggregate;
import com.bohaisoft.dal.client.aggregate.constant.SumType;
import org.springframework.util.CollectionUtils;



/** 
 * 求和
 * @author meiwei
 * @date 2014-1-26 上午11:15:26
 */
public class Sum implements IAggregate {
	private SumType sumType;
	
	public Sum(SumType sumType){
		this.sumType = sumType;
	}

	@Override
	public Object execute(List<Object> list,Integer dsSize){
		return execute(list);
	}
	
	@Override
	public Object execute(List<Object> list) {
		if (null == sumType) {
			return null;
		}
		// 整数运算
		if (sumType.ordinal() == SumType.INTEGER.ordinal()) {
			return sumInt(list);
		} 
		// 小数运算
		else if (sumType.ordinal() == SumType.DOUBLE.ordinal()) {
			return sumDouble(list);
		}
		return null;
	}
	
	private int sumInt(List<Object> list){
		int result = 0;
		if (CollectionUtils.isEmpty(list)) {
			return result;
		}
		for (Object value : list) {
			if (null == value) {
				continue;
			}
			result += (Integer) value;
		}
		return result;
	}
	
	private double sumDouble(List<Object> list){
		double result = 0;
		if (CollectionUtils.isEmpty(list)) {
			return result;
		}
		for (Object value : list) {
			if (null == value) {
				continue;
			}
			result += (Double) value;
		}
		return result;
	}
}
