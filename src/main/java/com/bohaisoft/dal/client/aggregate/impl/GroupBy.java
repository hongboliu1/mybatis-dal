package com.bohaisoft.dal.client.aggregate.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.bohaisoft.dal.client.aggregate.IAggregate;
import com.bohaisoft.dal.client.aggregate.ISupportGroupBy;
import org.apache.commons.collections4.CollectionUtils;


/**
 * 分组内聚
 * @author meiwei
 * @date 2014-1-26 下午02:01:56
 */
public class GroupBy implements IAggregate {
	
	private ISupportGroupBy supportGroupBy;
	
	public GroupBy(ISupportGroupBy supportGroupBy) {
		this.supportGroupBy = supportGroupBy;
	}

	@Override
	public Object execute(List<Object> list,Integer dsSize){
		return execute(list);
	}
	
	@Override
	public Object execute(List<Object> list) {
		if (null == supportGroupBy || CollectionUtils.isEmpty(list)) {
			return list;
		}
		Map<Object, Object> map = new HashMap<Object, Object>();
		for (Object value : list) {
			Object keyValue = supportGroupBy.getGroupBy(value);
			if (!map.containsKey(keyValue)) {
				map.put(keyValue, value);
			} else {
				Object base = map.get(keyValue);
				supportGroupBy.group(base, value);
			}
		}
		List<Object> resultList = new ArrayList<Object>();
		Set<Entry<Object, Object>> entrySet = map.entrySet();
		for (Entry<Object, Object> entry : entrySet) {
			resultList.add(entry.getValue());
		}
		return resultList;
	}
}
