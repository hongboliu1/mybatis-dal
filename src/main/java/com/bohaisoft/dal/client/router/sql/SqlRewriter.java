/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */
package com.bohaisoft.dal.client.router.sql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import com.bohaisoft.dal.client.exception.RoutingException;
import com.bohaisoft.dal.client.router.config.dataobject.ShardingMapping;
import com.bohaisoft.dal.client.router.support.RouterFactCtx;
import com.bohaisoft.dal.client.router.support.RouterFactCtxVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;



/**
 * 重写SQL，实现表名的替换
 * 
 * @author wuxiang
 * @since 2013-7-31
 */
public class SqlRewriter {

	static final Logger logger = LoggerFactory.getLogger(SqlRewriter.class);
	final String SQL_CONST_SEP = "'";
	final int MAX_PARAMETER_LENGTH = 10000;
	Map<String, ReplacedSql> replacedSqlCache = new HashMap<String, ReplacedSql>();
	Map<String, MergedSql> mergedSqlCache = new HashMap<String, MergedSql>();

	/**
	 * 替换SQL中的虚拟表为实际分表
	 * @param sql
	 * @param parameters
	 * @return
	 */
	public String replaceVirtualTable(String sql, Object[] parameters) {
		RouterFactCtxVO routerFact = RouterFactCtx.getRfvoholder();
		if (routerFact != null) {// 从线程变量里获取当前SQL的虚拟表名称和实际分表名称映射
			List<ShardingMapping> shardingMappingSet = routerFact.getVirtualTablesMapping();
			if (!CollectionUtils.isEmpty(shardingMappingSet)) {
				if (shardingMappingSet.size() > 1) {
					throw new RoutingException("more than one sharding mapping:" + shardingMappingSet + " with sql:" + sql);
				}
				Map<String, String> mapping = shardingMappingSet.iterator().next().getVirtualTablesMapping();
				if (!CollectionUtils.isEmpty(mapping)) {
					String newSql = replaceTable(sql, mapping);
					if (logger.isDebugEnabled()) {
						newSql = newSql.replaceAll("\\s{1,}", " ");
						logger.debug("new sql-" + newSql);
					}
					return newSql;
				}
			}
		}

		return sql;
	}

	public MergedSql getMergedSql(String statementName, String sql) {
		MergedSql mergedSql = null;
		if (statementName != null) {
			mergedSql = mergedSqlCache.get(statementName);
		}
		if (mergedSql == null) {
			SqlIteratorType sqlIteratorType = SqlIteratorType.OTHER;
			if (sql.matches("(?i).*\\?.*\\sor\\s.*\\?.*")) {
				sqlIteratorType = SqlIteratorType.OR;
			} else if (sql.matches("(?i).*\\sin\\s{0,}\\(.*\\?.*\\).*")) {
				sqlIteratorType = SqlIteratorType.IN;
			}
			mergedSql = new MergedSql();
			mergedSql.setSqlIteratorType(sqlIteratorType);
			if (statementName != null) {
				mergedSqlCache.put(statementName, mergedSql);
			}
		}

		return mergedSql;
	}

	/**
	 * 针对需要合并的分片查询（同一个库的不同分表的union all），替换SQL中的虚拟表为实际分表
	 * @param sql
	 * @param parameters
	 * @return
	 */
	public String replaceVirtualTableWithMergeShardingQueries(String sql, Object[] parameters) {
		RouterFactCtxVO routerFact = RouterFactCtx.getRfvoholder();
		if (routerFact != null) {// 从线程变量里获取当前SQL的虚拟表名称和实际分表名称映射
			List<ShardingMapping> shardingMappingSet = routerFact.getVirtualTablesMapping();
			if (shardingMappingSet != null) {
				if (shardingMappingSet.size() == 1) {
					Map<String, String> mapping = shardingMappingSet.iterator().next().getVirtualTablesMapping();
					if (!CollectionUtils.isEmpty(mapping)) {
						String newSql = replaceTable(sql, mapping);
						if (logger.isDebugEnabled()) {
							newSql = newSql.replaceAll("\\s{1,}", " ");
							logger.debug("new sql-" + newSql);
						}
						return newSql;
					}
				} else if (shardingMappingSet.size() > 1) {
					MergedSql mergedSql = getMergedSql(routerFact.getStatementName(), sql);
					String idxStr = ",";
					if (mergedSql.getSqlIteratorType().equals(SqlIteratorType.OR)) {
						idxStr = " OR ";
					}
					StringBuilder newSql = new StringBuilder();
					int paramCount = 0;
					for (ShardingMapping shardingMapping : shardingMappingSet) {
						List<Object> parameterObjects = shardingMapping.getParameterObjects();
						if (parameterObjects != null) {
							paramCount += parameterObjects.size();
						}
					}
					if (paramCount > 0 && parameters != null) {
						int paramCountOfEachObject = parameters.length / paramCount;
						String tailSql = mergedSql.getTailSql();
						for (ShardingMapping shardingMapping : shardingMappingSet) {
							Map<String, String> mapping = shardingMapping.getVirtualTablesMapping();
							if (!CollectionUtils.isEmpty(mapping)) {
								List<Object> parameterObjects = shardingMapping.getParameterObjects();
								String replaceTabSql = replaceTableWithMergeShardingQueries(routerFact.getStatementName(), sql, parameters.length, mapping);
								if (CollectionUtils.isEmpty(parameterObjects) && !CollectionUtils.isEmpty(shardingMapping.getInitialParameterObjects())) {
									continue;
								}
								int size = parameterObjects.size() * paramCountOfEachObject;
								String last = replaceTabSql;
								int start = -1;
								int count = 0;
								newSql.append("(");
								while ((start = last.indexOf("?")) != -1) {
									count++;
									if (count > size) {
										if (count > MAX_PARAMETER_LENGTH) {
											throw new RoutingException("more than " + MAX_PARAMETER_LENGTH + " parameters with statement:" + routerFact.getStatementName()
													+ ",sql:" + sql + ",parameters length:" + parameters.length);
										}
										int idx = last.indexOf(idxStr);
										if (idx != -1) {
											if (count == size + 1) {
												newSql.append(last.substring(0, idx));
											}
											if (tailSql != null) {
												last = tailSql;
												continue;
											}
											if (mergedSql.getSqlIteratorType().equals(SqlIteratorType.OR)) {
												int idx2 = last.indexOf(")", idx);
												if (idx2 != -1) {
													int idx1 = last.indexOf("(", idx);
													if (idx1 != -1 && idx1 < idx2) {
														int idx3 = last.indexOf(")", last.lastIndexOf("?"));
														if (idx3 != -1) {
															last = last.substring(idx3 + 1);
															tailSql = last;
															mergedSql.setTailSql(tailSql);
														} else {
															last = last.substring(idx2 + 1);
														}
													} else {
														last = last.substring(idx2);
														tailSql = last;
														mergedSql.setTailSql(tailSql);
													}
												} else {
													last = last.substring(last.lastIndexOf("?") + 1);
													tailSql = last;
													mergedSql.setTailSql(tailSql);
												}
											} else {
												int idx2 = last.indexOf(")", idx);
												if (idx2 != -1) {
													int idx1 = last.indexOf("(", idx);
													if (idx1 != -1 && idx1 < idx2) {
														int idx3 = last.indexOf(")", last.lastIndexOf("?"));
														if (idx3 != -1) {
															last = last.substring(idx3 + 1);
															tailSql = last;
															mergedSql.setTailSql(tailSql);
														} else {
															last = last.substring(idx2 + 1);
														}
													} else {
														last = last.substring(idx2);
														tailSql = last;
														mergedSql.setTailSql(tailSql);
													}
												} else {
													throw new RoutingException("error while parsing with statement:" + routerFact.getStatementName() + ",sql:" + sql
															+ ",parameters:" + parameters);
												}
											}
										} else {
											last = last.substring(last.lastIndexOf("?") + 1);
											tailSql = last;
											mergedSql.setTailSql(tailSql);
											// newSql.append("null");
											// last = last.substring(start + 1);
										}
									} else {
										newSql.append(last.substring(0, start));
										newSql.append("?");
										last = last.substring(start + 1);
									}
								}
								newSql.append(last).append(") union all ");
							}
						}
						if (newSql.length() > 0) {
							newSql.delete(newSql.length() - 11, newSql.length());
							if (logger.isDebugEnabled()) {
								logger.debug("new sql-" + newSql.toString());
							}
							return newSql.toString();
						}
					}
				}
			}
		}
		return sql;
	}

	/**
	 * 替换SQL中的表名
	 * 
	 * @param statementName
	 * @param sql
	 * @param virtualTablesMapping
	 * @return
	 */
	public String replaceTableWithMergeShardingQueries(String statementName, String sql, int parametersLength, Map<String, String> virtualTablesMapping) {
		if (!CollectionUtils.isEmpty(virtualTablesMapping)) {
			String cacheKey = null;
			if (statementName != null) {
				cacheKey = statementName + "#" + virtualTablesMapping;
			}
			ReplacedSql replacedSql = null;
			if (cacheKey != null) {
				replacedSql = replacedSqlCache.get(cacheKey);
				if (replacedSql != null && replacedSql.getParametersLength() >= parametersLength) {
					return replacedSql.getSql();
				}
			}
			StringBuilder newSql = new StringBuilder();
			StringTokenizer st = new StringTokenizer(sql, SQL_CONST_SEP, true);// 分析SQL常量分隔符'
			boolean isConst = false;

			while (st.hasMoreTokens()) {
				String nextToken = st.nextToken();
				if (SQL_CONST_SEP.equals(nextToken)) {
					isConst = !isConst;
				}
				String str = nextToken;
				if (!isConst) {
					for (String table : virtualTablesMapping.keySet()) {
						String replacedTable = virtualTablesMapping.get(table);
						str = str.replaceAll("(?i)\\s" + table + "\\s", " " + replacedTable + " ");// 替换表名前后是空字符的情况
						str = str.replaceAll("(?i)\\s" + table + "\\(", " " + replacedTable + "(");// 替换表名后面是(的情况
						str = str.replaceAll("(?i)\\s" + table + "\\)", " " + replacedTable + ")");// 替换表名后面是)的情况
						str = str.replaceAll("(?i)\\sor\\s", " OR ");//
						str = str.replaceAll("\\s{1,}", " ");
					}
				}
				newSql.append(str);
			}
			// System.out.println("replaceStringCache size:" +
			// replaceStringCache.size());
			String newSqlStr = newSql.toString();
			if (cacheKey != null) {
				replacedSql = new ReplacedSql();
				replacedSql.setSql(newSqlStr);
				replacedSql.setParametersLength(parametersLength);
				replacedSqlCache.put(cacheKey, replacedSql);
			}
			return newSqlStr;
		}

		return sql;
	}

	/**
	 * 替换SQL中的表名
	 * 
	 * @param sql
	 * @param virtualTablesMapping
	 * @return
	 */
	public String replaceTable(String sql, Map<String, String> virtualTablesMapping) {
		if (!CollectionUtils.isEmpty(virtualTablesMapping)) {
			StringBuilder newSql = new StringBuilder();
			StringTokenizer st = new StringTokenizer(sql, SQL_CONST_SEP, true);// 分析SQL常量分隔符'
			boolean isConst = false;

			while (st.hasMoreTokens()) {
				String nextToken = st.nextToken();
				if (SQL_CONST_SEP.equals(nextToken)) {
					isConst = !isConst;
				}
				String str = nextToken;
				if (!isConst) {
					for (String table : virtualTablesMapping.keySet()) {
						String replacedTable = virtualTablesMapping.get(table);
						str = str.replaceAll("(?i)\\s" + table + "\\s", " " + replacedTable + " ");// 替换表名前后是空字符的情况
						str = str.replaceAll("(?i)\\s" + table + "\\(", " " + replacedTable + "(");// 替换表名后面是(的情况
						str = str.replaceAll("(?i)\\s" + table + "\\)", " " + replacedTable + ")");// 替换表名后面是)的情况
						//str = str.replaceAll("\\s{1,}", " ");
						str = str.replaceAll("(?i),\\s{0,}" + table + "\\s", "," + replacedTable + " ");
					}
				}
				newSql.append(str);
			}
			return newSql.toString();
		}

		return sql;
	}
}
