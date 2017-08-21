package com.bohaisoft.dal.client.router.support;

import com.bohaisoft.dal.client.router.config.dataobject.ShardingMapping;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 设置路由策略的Helper类,方便客户端调用
 * 
 * @author wuxiang
 * @since 2012-7-5
 */
public class RouterFactHelper {

	public static RouterFactCtxVO getCurrentFact() {
		RouterFactCtxVO fact = RouterFactCtx.getRfvoholder();
		if (fact == null) {
			fact = new RouterFactCtxVO();
			RouterFactCtx.setRfvoholder(fact);
		}
		return fact;
	}

	/**
	 * 清除当前所有的路由策略
	 */
	public static void remove() {
		RouterFactCtx.remove();
	}

	/**
	 * 设置数据源组id
	 * 
	 * @param groupDS
	 */
	public static void setGroupDS(String groupDS) {
		getCurrentFact().setGroupDSName(groupDS);
	}

	/**
	 * 设置数据源组id
	 * 
	 * @param groupDS
	 * @param keepInThread
	 *            是否在线程中一直保持设置
	 */
	public static void setGroupDS(String groupDS, boolean keepInThread) {
		RouterFactCtxVO fact = getCurrentFact();
		Map<RouterFactCtxVO.RouterFact, Boolean> keepInThreadFacts = fact.getKeepInThreadFacts();
		if (keepInThreadFacts == null) {
			keepInThreadFacts = new HashMap<RouterFactCtxVO.RouterFact, Boolean>();
			fact.setKeepInThreadFacts(keepInThreadFacts);
		}
		keepInThreadFacts.put(RouterFactCtxVO.RouterFact.GROUPDS, keepInThread ? Boolean.TRUE : Boolean.FALSE);

		fact.setGroupDSName(groupDS);
	}

	/**
	 * 是否走主库
	 * 
	 * @param master
	 *            ，不设置该属性则默认为true
	 */
	public static void setMaster(boolean master) {
		getCurrentFact().setMdbFlag(master);
	}

	/**
	 * 是否走主库
	 * 
	 * @param master
	 *            ，不设置该属性则默认为true
	 * @param keepInThread
	 *            是否在线程中一直保持设置
	 */
	public static void setMaster(boolean master, boolean keepInThread) {
		RouterFactCtxVO fact = getCurrentFact();
		Map<RouterFactCtxVO.RouterFact, Boolean> keepInThreadFacts = fact.getKeepInThreadFacts();
		if (keepInThreadFacts == null) {
			keepInThreadFacts = new HashMap<RouterFactCtxVO.RouterFact, Boolean>();
			fact.setKeepInThreadFacts(keepInThreadFacts);
		}
		keepInThreadFacts.put(RouterFactCtxVO.RouterFact.IS_MASTER, keepInThread ? Boolean.TRUE : Boolean.FALSE);

		fact.setMdbFlag(master);
	}

	/**
	 * 设置虚拟表名与实际分表名称映射
	 * 
	 * @param virtualTablesMapping
	 */
	public static void setVirtualTablesMapping(List<ShardingMapping> virtualTablesMapping) {
		getCurrentFact().setVirtualTablesMapping(virtualTablesMapping);
	}

	/**
	 * 设置虚拟表名与实际分表名称映射
	 * 
	 * @param virtualTablesMapping
	 * @param keepInThread
	 *            是否在线程中一直保持设置
	 */
	public static void setVirtualTablesMapping(List<ShardingMapping> virtualTablesMapping, boolean keepInThread) {
		RouterFactCtxVO fact = getCurrentFact();
		Map<RouterFactCtxVO.RouterFact, Boolean> keepInThreadFacts = fact.getKeepInThreadFacts();
		if (keepInThreadFacts == null) {
			keepInThreadFacts = new HashMap<RouterFactCtxVO.RouterFact, Boolean>();
			fact.setKeepInThreadFacts(keepInThreadFacts);
		}
		keepInThreadFacts.put(RouterFactCtxVO.RouterFact.VIRTUAL_TABLES_MAPPING, keepInThread ? Boolean.TRUE : Boolean.FALSE);

		fact.setVirtualTablesMapping(virtualTablesMapping);
	}

	/**
	 * 获得当前设置的数据源组id
	 */
	public static String getGroupDS() {
		return getCurrentFact().getGroupDSName();
	}

	/**
	 * 是否走主库
	 */
	public static boolean isMaster() {
		if (getCurrentFact().getMdbFlag() == null) {
			return true;
		} else {
			return getCurrentFact().getMdbFlag().booleanValue();
		}
	}

	/**
	 * 获得当前设置的虚拟表名与实际分表名称映射
	 */
	public static List<ShardingMapping> getVirtualTablesMapping() {
		return getCurrentFact().getVirtualTablesMapping();
	}

	/**
	 * 是否并行执行update
	 * 
	 * @param executeUpdateInConcurrency
	 */
	public static void setExecuteUpdateInConcurrency(boolean executeUpdateInConcurrency) {
		getCurrentFact().setExecuteUpdateInConcurrency(executeUpdateInConcurrency);
	}

	/**
	 * 是否并行执行update
	 * 
	 * @param executeUpdateInConcurrency
	 * @param keepInThread
	 *            是否在线程中一直保持设置
	 */
	public static void setExecuteUpdateInConcurrency(boolean executeUpdateInConcurrency, boolean keepInThread) {
		RouterFactCtxVO fact = getCurrentFact();
		Map<RouterFactCtxVO.RouterFact, Boolean> keepInThreadFacts = fact.getKeepInThreadFacts();
		if (keepInThreadFacts == null) {
			keepInThreadFacts = new HashMap<RouterFactCtxVO.RouterFact, Boolean>();
			fact.setKeepInThreadFacts(keepInThreadFacts);
		}
		keepInThreadFacts.put(RouterFactCtxVO.RouterFact.EXECUTE_UPDATE_IN_CONCURRENCY, keepInThread ? Boolean.TRUE : Boolean.FALSE);

		fact.setExecuteUpdateInConcurrency(executeUpdateInConcurrency);
	}

	/**
	 * 是否并行执行insert
	 * 
	 * @param executeInsertInConcurrency
	 */
	public static void setExecuteInsertInConcurrency(boolean executeInsertInConcurrency) {
		getCurrentFact().setExecuteInsertInConcurrency(executeInsertInConcurrency);
	}

	/**
	 * 是否并行执行insert
	 * 
	 * @param executeInsertInConcurrency
	 * @param keepInThread
	 *            是否在线程中一直保持设置
	 */
	public static void setExecuteInsertInConcurrency(boolean executeInsertInConcurrency, boolean keepInThread) {
		RouterFactCtxVO fact = getCurrentFact();
		Map<RouterFactCtxVO.RouterFact, Boolean> keepInThreadFacts = fact.getKeepInThreadFacts();
		if (keepInThreadFacts == null) {
			keepInThreadFacts = new HashMap<RouterFactCtxVO.RouterFact, Boolean>();
			fact.setKeepInThreadFacts(keepInThreadFacts);
		}
		keepInThreadFacts.put(RouterFactCtxVO.RouterFact.EXECUTE_INSERT_IN_CONCURRENCY, keepInThread ? Boolean.TRUE : Boolean.FALSE);

		fact.setExecuteInsertInConcurrency(executeInsertInConcurrency);
	}

	/**
	 * 是否并行执行delete
	 * 
	 * @param executeDeleteInConcurrency
	 */
	public static void setExecuteDeleteInConcurrency(boolean executeDeleteInConcurrency) {
		getCurrentFact().setExecuteDeleteInConcurrency(executeDeleteInConcurrency);
	}

	/**
	 * 是否并行执行delete
	 * 
	 * @param executeDeleteInConcurrency
	 * @param keepInThread
	 *            是否在线程中一直保持设置
	 */
	public static void setExecuteDeleteInConcurrency(boolean executeDeleteInConcurrency, boolean keepInThread) {
		RouterFactCtxVO fact = getCurrentFact();
		Map<RouterFactCtxVO.RouterFact, Boolean> keepInThreadFacts = fact.getKeepInThreadFacts();
		if (keepInThreadFacts == null) {
			keepInThreadFacts = new HashMap<RouterFactCtxVO.RouterFact, Boolean>();
			fact.setKeepInThreadFacts(keepInThreadFacts);
		}
		keepInThreadFacts.put(RouterFactCtxVO.RouterFact.EXECUTE_DELETE_IN_CONCURRENCY, keepInThread ? Boolean.TRUE : Boolean.FALSE);

		fact.setExecuteDeleteInConcurrency(executeDeleteInConcurrency);
	}

	/**
	 * 是否并行执行query
	 * 
	 * @param executeQueryInConcurrency
	 */
	public static void setExecuteQueryInConcurrency(boolean executeQueryInConcurrency) {
		getCurrentFact().setExecuteQueryInConcurrency(executeQueryInConcurrency);
	}

	/**
	 * 是否并行执行query
	 * 
	 * @param executeQueryInConcurrency
	 * @param keepInThread
	 *            是否在线程中一直保持设置
	 */
	public static void setExecuteQueryInConcurrency(boolean executeQueryInConcurrency, boolean keepInThread) {
		RouterFactCtxVO fact = getCurrentFact();
		Map<RouterFactCtxVO.RouterFact, Boolean> keepInThreadFacts = fact.getKeepInThreadFacts();
		if (keepInThreadFacts == null) {
			keepInThreadFacts = new HashMap<RouterFactCtxVO.RouterFact, Boolean>();
			fact.setKeepInThreadFacts(keepInThreadFacts);
		}
		keepInThreadFacts.put(RouterFactCtxVO.RouterFact.EXECUTE_QUERY_IN_CONCURRENCY, keepInThread ? Boolean.TRUE : Boolean.FALSE);

		fact.setExecuteQueryInConcurrency(executeQueryInConcurrency);
	}
}
