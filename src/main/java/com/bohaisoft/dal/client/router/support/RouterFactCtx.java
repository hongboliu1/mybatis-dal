package com.bohaisoft.dal.client.router.support;

import org.springframework.util.CollectionUtils;

import java.util.Map;


/**
 * Created by IntelliJ IDEA. User: Administrator Date: 12-2-24 Time: 下午1:10 To
 * change this template use File | Settings | File Templates.
 */
public class RouterFactCtx {
    private static ThreadLocal<RouterFactCtxVO> rfvoholder = new ThreadLocal<>();// 私有静态变量

    public static RouterFactCtxVO getRfvoholder() {
        return rfvoholder.get();
    }

    public static void setRfvoholder(RouterFactCtxVO rfvo) {
        rfvoholder.set(rfvo);
    }

    public static void clearRfvoholder() {
        RouterFactCtxVO value = getRfvoholder();
        boolean clearMdbFlag = true;
        boolean clearGroupDSName = true;
        boolean clearVirtualTablesMapping = true;
        boolean clearExecuteDeleteInConcurrency = true;
        boolean clearExecuteUpdateInConcurrency = true;
        boolean clearExecuteInsertInConcurrency = true;
        boolean clearExecuteQueryInConcurrency = true;
        if (value != null) {
            Map<RouterFactCtxVO.RouterFact, Boolean> facts = value.getKeepInThreadFacts();
            if (!CollectionUtils.isEmpty(facts)) {
                for (RouterFactCtxVO.RouterFact fact : facts.keySet()) {
                    Boolean isKeep = facts.get(fact);
                    if (isKeep != null && isKeep) {
                        if (RouterFactCtxVO.RouterFact.IS_MASTER.equals(fact)) {
                            clearMdbFlag = false;
                        } else if (RouterFactCtxVO.RouterFact.GROUPDS.equals(fact)) {
                            clearGroupDSName = false;
                        } else if (RouterFactCtxVO.RouterFact.VIRTUAL_TABLES_MAPPING.equals(fact)) {
                            clearVirtualTablesMapping = false;
                        } else if (RouterFactCtxVO.RouterFact.EXECUTE_DELETE_IN_CONCURRENCY.equals(fact)) {
                            clearExecuteDeleteInConcurrency = false;
                        } else if (RouterFactCtxVO.RouterFact.EXECUTE_UPDATE_IN_CONCURRENCY.equals(fact)) {
                            clearExecuteUpdateInConcurrency = false;
                        } else if (RouterFactCtxVO.RouterFact.EXECUTE_INSERT_IN_CONCURRENCY.equals(fact)) {
                            clearExecuteInsertInConcurrency = false;
                        } else if (RouterFactCtxVO.RouterFact.EXECUTE_QUERY_IN_CONCURRENCY.equals(fact)) {
                            clearExecuteQueryInConcurrency = false;
                        }
                    }
                }
            }

            if (clearMdbFlag) {
                value.setMdbFlag(null);
            }
            if (clearGroupDSName) {
                value.setGroupDSName(null);
            }
            if (clearVirtualTablesMapping) {
                value.clearVirtualTablesMapping();
            }
            if (clearExecuteDeleteInConcurrency) {
                value.setExecuteDeleteInConcurrency(null);
            }
            if (clearExecuteUpdateInConcurrency) {
                value.setExecuteUpdateInConcurrency(null);
            }
            if (clearExecuteInsertInConcurrency) {
                value.setExecuteInsertInConcurrency(null);
            }
            if (clearExecuteQueryInConcurrency) {
                value.setExecuteQueryInConcurrency(null);
            }
        }
        if (clearMdbFlag && clearGroupDSName && clearVirtualTablesMapping && clearExecuteDeleteInConcurrency && clearExecuteUpdateInConcurrency && clearExecuteInsertInConcurrency
                && clearExecuteQueryInConcurrency) {
            rfvoholder.remove();
        }
    }

    public static void remove() {
        rfvoholder.remove();
    }
}
