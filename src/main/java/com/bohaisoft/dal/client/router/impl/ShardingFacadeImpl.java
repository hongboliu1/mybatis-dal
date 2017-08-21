/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router.impl;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.bohaisoft.dal.client.exception.RoutingException;
import com.bohaisoft.dal.client.router.ShardingFacade;
import com.bohaisoft.dal.client.router.config.dataobject.ShardingFactDO;
import com.bohaisoft.dal.client.router.config.dataobject.ShardingMapping;
import com.bohaisoft.dal.client.router.config.dataobject.ShardingRuleContainer;
import com.bohaisoft.dal.client.router.config.dataobject.ShardingRuleOfTable;
import com.bohaisoft.dal.client.router.rule.ShardingRule;
import com.bohaisoft.dal.client.router.rule.support.RuleValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;

/**
 * 
 * 
 * @author wuxiang
 * @since 2012-3-12
 */
public class ShardingFacadeImpl implements ShardingFacade, InitializingBean, DisposableBean {

	private ShardingRuleContainer shardingRuleContainer;
	static final Logger logger = LoggerFactory.getLogger(ShardingFacadeImpl.class);

	public ShardingRuleContainer getShardingRuleContainer() {
		return shardingRuleContainer;
	}

	public void setShardingRuleContainer(ShardingRuleContainer shardingRuleContainer) {
		this.shardingRuleContainer = shardingRuleContainer;
	}

	public Map<String, List<ShardingMapping>> decideTargetDatasourceGroups(ShardingFactDO shardingFact) {
		Map<String, List<ShardingMapping>> groups = new HashMap<String, List<ShardingMapping>>();
		// 首先从垂直分库规则里查找是否有匹配的数据源
		Map<String, Collection<String>> verticalShardingRules = shardingRuleContainer.getVerticalShardingRules();
		List<ShardingMapping> virtualTablesMapping = shardingFact.getVirtualTablesMapping();
		if (verticalShardingRules != null) {
			Collection<String> virtualTables = shardingFact.getTables();
			if (!CollectionUtils.isEmpty(virtualTablesMapping) || !CollectionUtils.isEmpty(virtualTables)) {
				for (String group : verticalShardingRules.keySet()) {
					Collection<String> values = verticalShardingRules.get(group);
					if (values != null) {
						if (virtualTablesMapping != null) {
							for (ShardingMapping mapping : virtualTablesMapping) {
								Map<String, String> virtualTabMapping = mapping.getVirtualTablesMapping();
								if (virtualTabMapping != null) {
									int matchCount = 0;
									for (String actualTab : virtualTabMapping.values()) {
										String tab = actualTab;
										if (actualTab != null && shardingRuleContainer.isIgnoreCase()) {
											tab = actualTab.toUpperCase();
										}
										if (values.contains(tab)) {
											matchCount++;
										}
									}
									if (matchCount > 0 && matchCount != virtualTabMapping.size()) {
										throw new RoutingException("Inconsistent sharding mapping:" + mapping);
									}
									if (matchCount > 0) {
										List<ShardingMapping> mappings = groups.get(group);
										if (mappings == null) {
											mappings = new ArrayList<ShardingMapping>();
											groups.put(group, mappings);
										}
										if (!mappings.contains(mapping)) {
											mappings.add(mapping);
										}
									}
								}
							}
						}
						if (virtualTables != null) {
							for (String virtualTab : virtualTables) {
								String tab = virtualTab;
								if (virtualTab != null && shardingRuleContainer.isIgnoreCase()) {
									tab = virtualTab.toUpperCase();
								}
								if (values.contains(tab)) {
									if (!groups.containsKey(group)) {
										groups.put(group, null);
									}
								}
							}
						}
					}
				}
				if (!groups.isEmpty()) {
					for(String group : groups.keySet()) {
						List<ShardingMapping> mappings = groups.get(group);
						if(mappings != null && mappings.size() > 1) {
							Collections.sort(mappings);
						}
					}
					return groups;
				}
			}
		}
		// 从水平分库规则里查找是否有匹配的数据源
		Map<String, ShardingRuleOfTable> horizonalShardingRules = shardingRuleContainer.getHorizontalShardingRules();
		if (horizonalShardingRules != null) {
			Collection<String> tables = shardingFact.getTables();
			if (!CollectionUtils.isEmpty(tables)) {
				String firstTable = tables.iterator().next();
				if(shardingRuleContainer.isIgnoreCase()) {
					firstTable = firstTable.toUpperCase();
				}
				ShardingRuleOfTable ruleValue = horizonalShardingRules.get(firstTable);
				if (ruleValue != null) {
					List<ShardingRule> rules = ruleValue.getDbRuleList();
					if (rules != null) {
						for (ShardingRule rule : rules) {
							Object parameterObject = shardingFact.getParameterObject();
							if (parameterObject instanceof Map) {// for Map,
								Object list = ((Map)parameterObject).get("list");
								if(list instanceof Collection || list != null && list.getClass().isArray()) {
									parameterObject = list;
								}
							}
							
							if (parameterObject instanceof Collection) {// for										// Collection,
								Map<ShardingMapping, List<Object>> mappingParameters = new HashMap<ShardingMapping, List<Object>>();
								Collection parameterList = (Collection) parameterObject;
								Iterator ir = parameterList.iterator();
								while (ir.hasNext()) {
									Object nextParam = ir.next();
									String shard = rule.decideTargetShards(nextParam);
									if (shard != null) {
										if (logger.isDebugEnabled()) {
											logger.debug("Target shards-" + shard + " with rule-" + rule + ",parameters-" + shardingFact);
										}
										List<ShardingMapping> mappings = groups.get(shard);
										if (mappings == null) {
											mappings = new ArrayList<ShardingMapping>();
											groups.put(shard, mappings);
										}
										if (CollectionUtils.isEmpty(virtualTablesMapping)) {
											if (mappings.isEmpty()) {
												ShardingMapping sm = new ShardingMapping();
												mappings.add(sm);
											}
											ShardingMapping sm = mappings.get(0);
											List<Object> params = sm.getParameterObjects();
											if (params == null) {
												params = new ArrayList<Object>();
												sm.setParameterObjects(params);
											}
											params.add(nextParam);
										} else {
											boolean isContain = false;
											if (!mappings.isEmpty()) {
												for (ShardingMapping sm : mappings) {
													List<Object> initialParams = sm.getInitialParameterObjects();
													if (initialParams.contains(nextParam)) {
														isContain = true;
														List<Object> params = sm.getParameterObjects();
														if (params == null) {
															params = new ArrayList<Object>();
															sm.setParameterObjects(params);
														}
														params.add(nextParam);
													}
												}
											}
											if (!isContain) {
												for (ShardingMapping mapping : virtualTablesMapping) {
													if (mapping.getParameterObjects().contains(nextParam)) {
														ShardingMapping sm = new ShardingMapping();
														sm.setVirtualTablesMapping(mapping.getVirtualTablesMapping());
														sm.setInitialParameterObjects(mapping.getParameterObjects());
														mappings.add(sm);
														List<Object> params = new ArrayList<Object>();
														sm.setParameterObjects(params);
														params.add(nextParam);
														break;
													}
												}
											}
										}
									}
								}
							} else if (parameterObject != null && parameterObject.getClass().isArray()) {
								int len = Array.getLength(parameterObject);
								Map<ShardingMapping, List<Object>> mappingParameters = new HashMap<ShardingMapping, List<Object>>();
								for (int i = 0; i < len; i++) {
									Object nextParam = Array.get(parameterObject, i);
									String shard = rule.decideTargetShards(nextParam);
									if (shard != null) {
										if (logger.isDebugEnabled()) {
											logger.debug("Target shards-" + shard + " with rule-" + rule + ",parameters-" + shardingFact.getParameterObject());
										}
										List<ShardingMapping> mappings = groups.get(shard);
										if (mappings == null) {
											mappings = new ArrayList<ShardingMapping>();
											groups.put(shard, mappings);
										}
										if (CollectionUtils.isEmpty(virtualTablesMapping)) {
											if (mappings.isEmpty()) {
												ShardingMapping sm = new ShardingMapping();
												mappings.add(sm);
											}
											ShardingMapping sm = mappings.get(0);
											List<Object> params = sm.getParameterObjects();
											if (params == null) {
												params = new ArrayList<Object>();
												sm.setParameterObjects(params);
											}
											params.add(nextParam);
										} else {
											boolean isContain = false;
											if (!mappings.isEmpty()) {
												for (ShardingMapping sm : mappings) {
													List<Object> initialParams = sm.getInitialParameterObjects();
													if (initialParams.contains(nextParam)) {
														isContain = true;
														List<Object> params = sm.getParameterObjects();
														if (params == null) {
															params = new ArrayList<Object>();
															sm.setParameterObjects(params);
														}
														params.add(nextParam);
													}
												}
											}
											if (!isContain) {
												for (ShardingMapping mapping : virtualTablesMapping) {
													if (mapping.getParameterObjects().contains(nextParam)) {
														ShardingMapping sm = new ShardingMapping();
														sm.setVirtualTablesMapping(mapping.getVirtualTablesMapping());
														sm.setInitialParameterObjects(mapping.getParameterObjects());
														mappings.add(sm);
														List<Object> params = new ArrayList<Object>();
														sm.setParameterObjects(params);
														params.add(nextParam);
														break;
													}
												}
											}
										}
									}
								}
							} else {
								String shard = rule.decideTargetShards(parameterObject);
								if (shard != null) {
									String[] shards = shard.split(",");
									if(shards.length>1){
										for(String ashard:shards){
											int len = 0;
											List<Object> arrayList = new  ArrayList<Object>();
											if (parameterObject instanceof Map) {// for
//												Collection parameterList = (Collection) parameterObject;
												Collection parameterList =((Map) parameterObject).values();
												Iterator ir = parameterList.iterator();
					                             while (ir.hasNext()) {
						                         Object nextParam = ir.next();
						                         len++;
						                         if(nextParam==null||nextParam.toString().indexOf(",")==-1)
						                         arrayList.add(nextParam);
						                         else{
						                        	 for(String param:nextParam.toString().split(",")){
						                        		 arrayList.add(param);
						                        	 }
						                         }
					                             }
											}else{
												String[] paramStr= parameterObject.toString().split(",");
												for(String param:paramStr){
							                         arrayList.add(param);
												}
											}

											len = arrayList.size();
											List<ShardingMapping> mappings = groups.get(ashard);
											if (mappings == null) {
												mappings = new ArrayList<ShardingMapping>();
												groups.put(ashard, mappings);
											}
											for (int i = 0; i < len; i++) {
											    Object nextParam = arrayList.get(i);
												if (ashard != null) {
													if (logger.isDebugEnabled()) {
														logger.debug("Target shards-" + ashard + " with rule-" + rule + ",parameters-" + shardingFact.getParameterObject());
													}
													if (CollectionUtils.isEmpty(virtualTablesMapping)) {
														if (mappings.isEmpty()) {
															ShardingMapping sm = new ShardingMapping();
															mappings.add(sm);
														}
														ShardingMapping sm = mappings.get(0);
														List<Object> params = sm.getParameterObjects();
														if (params == null) {
															params = new ArrayList<Object>();
															sm.setParameterObjects(params);
														}
														params.add(nextParam);
													} else {
														boolean isContain = false;
														if (!mappings.isEmpty()) {
															for (ShardingMapping sm : mappings) {
																List<Object> initialParams = sm.getInitialParameterObjects();
																if (initialParams.contains(nextParam)) {
																	isContain = true;
																	List<Object> params = sm.getParameterObjects();
																	if (params == null) {
																		params = new ArrayList<Object>();
																		sm.setParameterObjects(params);
																	}
																	params.add(nextParam);
																}
															}
														}
														if (!isContain) {
															for (ShardingMapping mapping : virtualTablesMapping) {
																if (mapping.getParameterObjects().contains(nextParam)) {
																	ShardingMapping sm = new ShardingMapping();
																	sm.setVirtualTablesMapping(mapping.getVirtualTablesMapping());
																	sm.setInitialParameterObjects(mapping.getParameterObjects());
																	mappings.add(sm);
																	List<Object> params = new ArrayList<Object>();
																	sm.setParameterObjects(params);
																	params.add(nextParam);
																	break;
																}
															}
														}
													}
												}
											}
										}
										
									}else
									groups.put(shard, virtualTablesMapping);
								}							
							}
							if (!groups.isEmpty()) {
								break;
							}
						}
					}
				}
			}
		}
		for(String group : groups.keySet()) {
			List<ShardingMapping> mappings = groups.get(group);
			if(mappings != null && mappings.size() > 1) {
				Collections.sort(mappings);
			}
		}
		return groups;
	}

	public void afterPropertiesSet() throws Exception {
		if (getShardingRuleContainer() == null) {
			throw new IllegalArgumentException("The shardingRuleContainer property must be set");
		}

		// 校验垂直分库规则
		RuleValidator.validateShardingRules(shardingRuleContainer.getVerticalShardingRules());
	}

	public void destroy() throws Exception {

	}

	@Override
	public List<ShardingMapping> decideTargetTableNames(ShardingFactDO shardingFact) {
		Collection<String> tables = shardingFact.getTables();

		List<ShardingMapping> mappings = null;
		if (!CollectionUtils.isEmpty(tables)) {
			Object parameterObject = shardingFact.getParameterObject();
			if (parameterObject instanceof Map) {// for Map,
				Object list = ((Map)parameterObject).get("list");
				if(list instanceof Collection || list != null && list.getClass().isArray()) {
					parameterObject = list;
				}
			}
			if (parameterObject instanceof Collection) {// for Collection,
				Map<ShardingMapping, List<Object>> mappingParameters = new HashMap<ShardingMapping, List<Object>>();
				Collection parameterList = (Collection) parameterObject;
				Iterator ir = parameterList.iterator();
				while (ir.hasNext()) {
					Object nextParam = ir.next();
					ShardingMapping mapping = getShardingMappingOfParameterObject(tables, nextParam);
					if (mapping == null) {
						return null;
					}
					if (mappings == null) {
						mappings = new ArrayList<ShardingMapping>();
					}
					if (!mappings.contains(mapping)) {
						mappings.add(mapping);
					}
					List<Object> parameters = mappingParameters.get(mapping);
					if (parameters == null) {
						parameters = new ArrayList<Object>();
						mappingParameters.put(mapping, parameters);
					}
					parameters.add(nextParam);
				}
				if (mappings != null) {
					for (ShardingMapping mapping : mappings) {
						mapping.setParameterObjects(mappingParameters.get(mapping));
					}
				}
			} else if (parameterObject != null && parameterObject.getClass().isArray()) {
				int len = Array.getLength(parameterObject);
				Map<ShardingMapping, List<Object>> mappingParameters = new HashMap<ShardingMapping, List<Object>>();
				for (int i = 0; i < len; i++) {
					Object nextParam = Array.get(parameterObject, i);
					ShardingMapping mapping = getShardingMappingOfParameterObject(tables, nextParam);
					if (mapping == null) {
						return null;
					}
					if (mappings == null) {
						mappings = new ArrayList<ShardingMapping>();
					}
					if (!mappings.contains(mapping)) {
						mappings.add(mapping);
					}
					List<Object> parameters = mappingParameters.get(mapping);
					if (parameters == null) {
						parameters = new ArrayList<Object>();
						mappingParameters.put(mapping, parameters);
					}
					parameters.add(nextParam);
				}
				if (mappings != null) {
					for (ShardingMapping mapping : mappings) {
						mapping.setParameterObjects(mappingParameters.get(mapping));
					}
				}
			} else {
				ShardingMapping mapping = getShardingMappingOfParameterObject(tables, parameterObject);
				if (mapping != null) {
					mappings = new ArrayList<ShardingMapping>();
					if (mapping.isMultiple()) {
						Map<String, String> tableMapping = mapping.getVirtualTablesMapping();
						for (String table : tableMapping.keySet()) {
							String virtualTable = tableMapping.get(table);
							String[] vTabArray = virtualTable.split(",");
							int idx = 0;
							for (String vTab : vTabArray) {
								ShardingMapping shardingMapping = null;
								if (mappings.size() > idx) {
									shardingMapping = mappings.get(idx);
								} else {
									shardingMapping = new ShardingMapping();
									mappings.add(shardingMapping);
								}
								Map<String, String> virtualTablesMapping = shardingMapping.getVirtualTablesMapping();
								if (virtualTablesMapping == null) {
									virtualTablesMapping = new HashMap<String, String>();
									shardingMapping.setVirtualTablesMapping(virtualTablesMapping);
								}
								virtualTablesMapping.put(table, vTab);
								idx++;
							}
						}

					} else {
						mappings.add(mapping);
					}
				}
			}
		}
		if(mappings != null && mappings.size() > 1) {
			Collections.sort(mappings);
		}
		return mappings;
	}

	private ShardingMapping getShardingMappingOfParameterObject(Collection<String> tables, Object parameterObject) {
		ShardingMapping mapping = null;
		for (String virtualTable : tables) {
			Map<String, ShardingRuleOfTable> horizonalShardingRules = shardingRuleContainer.getHorizontalShardingRules();
			if (horizonalShardingRules != null) {
				String matchTable = virtualTable;
				if(shardingRuleContainer.isIgnoreCase()) {
					matchTable = matchTable.toUpperCase();
				}
				ShardingRuleOfTable ruleValue = horizonalShardingRules.get(matchTable);
				if (ruleValue != null) {
					List<ShardingRule> rules = ruleValue.getTbRuleList();
					if (rules != null) {
						for (ShardingRule rule : rules) {
							String shard = rule.decideTargetShards(parameterObject);
							if (shard != null) {
								if (logger.isDebugEnabled()) {
									logger.debug("Target shards-" + shard + " with rule-" + rule + ",parameters-" + parameterObject);
								}
								if (mapping == null) {
									mapping = new ShardingMapping();
									Map<String, String> virtualTablesMapping = new HashMap<String, String>();
									mapping.setVirtualTablesMapping(virtualTablesMapping);
								}
								if (shard.indexOf(",") != -1) {
									mapping.setMultiple(true);
								}
								mapping.getVirtualTablesMapping().put(virtualTable, shard);
								break;
							}
						}
					}
				}
			}
		}
		return mapping;
	}
}
