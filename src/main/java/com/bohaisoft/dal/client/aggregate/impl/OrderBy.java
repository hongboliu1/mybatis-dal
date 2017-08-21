package com.bohaisoft.dal.client.aggregate.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.bohaisoft.dal.client.aggregate.IAggregate;
import org.springframework.util.CollectionUtils;


/** 
 * 排序内聚
 * @author meiwei
 * @date 2014-1-26 下午03:15:00
 */
public class OrderBy implements IAggregate {
	@SuppressWarnings("rawtypes")
	private Comparator comparator;
	private Integer offset;
	private Integer pageSize;

	/**
	 * 构造函数
	 * @author meiwei
	 * @date 2014-1-28 上午10:58:32
	 * @param comparator 排序比较器
	 * @return Long
	 */
	@SuppressWarnings("rawtypes")
	public OrderBy(Comparator comparator) {
		this.comparator = comparator;
	}
	
	/**
	 * 构造函数
	 * @author meiwei
	 * @date 2014-1-28 上午10:58:32
	 * @param offset 分页偏移位
	 * @param pageSize 每页查询多少条数据
	 * @param comparator 排序比较器
	 * @return Long
	 */
	@SuppressWarnings("rawtypes")
	public OrderBy(Integer offset, Integer pageSize, Comparator comparator){
		this.comparator = comparator;
		this.offset = offset;
		this.pageSize = pageSize;
	}
	
	@Override
	public Object execute(List<Object> list){
		return execute(list,null);
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public Object execute(List<Object> list,Integer dsSize) {
		// 排序
		if (null != comparator && !CollectionUtils.isEmpty(list)) {
			Collections.sort(list, comparator);
		}
		// 返回list的情况
		List<Object> resultList = new ArrayList<Object>();
		// 分页处理
		if (null != offset && null != pageSize) {
			// 当前页没有数据
			if (list.size() == 0) {
				return resultList;
			}
			// 当前页有数据
			else {
				// 只取第一条，返回单个Object的情况
//				if (pageSize == 1 && offset == 0) {
//					return list.get(offset);
//				}
				if(list.size() <= pageSize){
					if (offset > 0 && dsSize != null && dsSize > 1){
						pageSize = 0;
					}
					offset = 0;
				} 
				
				// 返回list的情况
				for (int i = offset; i < list.size(); i++) {
					if (resultList.size() == pageSize) {
						break;
					}
					resultList.add(list.get(i));
				}
				return resultList;
			}
		}
		return list;
	}

}
