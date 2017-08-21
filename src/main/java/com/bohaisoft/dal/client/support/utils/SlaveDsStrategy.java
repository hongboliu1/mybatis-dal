package com.bohaisoft.dal.client.support.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * 根据权重策略 得到备库
 * 
 * 
 */
public class SlaveDsStrategy {

	/**
	 * 
	 * @param key：slaveDS  value：权重
	 * @return 返回的是slave名称 相当于ID
	 */
	public static String getSlaveId(Map<String,Integer> slaveDSMap) {
		int total = 0;
		
		String result = "";
		
		//数轴
		Map<String, Map<String, Integer>> axes = new HashMap<String, Map<String, Integer>>();

		// 构造数轴
		for (String slave : slaveDSMap.keySet()) {
			//数轴分段
			Map<String, Integer> section = new HashMap<String, Integer>();
			section.put("min", total);
			//获取出 权重值
			total += slaveDSMap.get(slave);
			section.put("max", total);
			axes.put(slave, section);
		}
		double fact = Math.random() * total;
		
		for (String keyA : axes.keySet()) {
			//分段
			Map<String, Integer> sectioin = axes.get(keyA);
			if (fact >= sectioin.get("min") && fact <= sectioin.get("max")) {
				result = keyA;
				break;
			}
		}
		return result;
	}

}
