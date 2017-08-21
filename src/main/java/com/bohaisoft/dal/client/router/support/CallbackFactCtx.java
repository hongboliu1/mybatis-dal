/**
 * Copyright(c) 2012 yihaodian. All rights reserved.
 * dal-client
 */

package com.bohaisoft.dal.client.router.support;


import com.bohaisoft.dal.client.router.config.dataobject.ShardingMapping;
import com.bohaisoft.dal.client.router.support.CallbackFactCtxVO.CallbackSessionFact;
import org.apache.ibatis.session.SqlSession;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Callback操作上下文
 *
 * @author wuxiang
 * @since 2012-5-21
 */
public class CallbackFactCtx {
    private static ThreadLocal<CallbackFactCtxVO> holder = new ThreadLocal<CallbackFactCtxVO>();// 私有静态变量

    public static CallbackFactCtxVO getFact() {
        return holder.get();
    }

    public static void setFact(CallbackFactCtxVO fact) {
        holder.set(fact);
    }

    public static void clear() {
        CallbackFactCtxVO fact = getFact();
        if (fact != null) {
            Map<String, CallbackSessionFact> sessions = fact.getSessions();
            for (CallbackSessionFact sessionFact : sessions.values()) {
                Connection connection = sessionFact.getConnection();
                SqlSession session = sessionFact.getSqlSessionProxy();
                DataSource dataSource = sessionFact.getDataSource();
                boolean transactionAware = (dataSource instanceof TransactionAwareDataSourceProxy);
                try {
                    if (connection != null) {
                        if (transactionAware) {
                            connection.close();
                        } else {
                            DataSourceUtils.doReleaseConnection(connection, dataSource);
                        }
                    }
                } catch (Throwable ex) {
                }
                session.close();
            }
            sessions.clear();
        }
        holder.remove();
    }

    public static void init(SqlSession sqlSessionProxy) {
        CallbackFactCtxVO fact = new CallbackFactCtxVO();
        fact.setSqlSessionProxy(sqlSessionProxy);
        setFact(fact);
    }

    public static String getFactKey(String dsId, RouterFactCtxVO routerFact) {
        String key = dsId;
        if (routerFact != null) {
            List<ShardingMapping> virtualTablesMapping = routerFact.getVirtualTablesMapping();
            if (!CollectionUtils.isEmpty(virtualTablesMapping)) {
                List<String> tablesSorted = new ArrayList<String>();
                for (ShardingMapping mapping : virtualTablesMapping) {
                    Map<String, String> tabMapping = mapping.getVirtualTablesMapping();
                    for (String tab : tabMapping.values()) {
                        tablesSorted.add(tab);
                    }
                }
                Collections.sort(tablesSorted);
                key = key + "_" + tablesSorted.toString();
            }
        }
        return key;
    }

    public static boolean addDataSourceIfAbsent(String dsId, RouterFactCtxVO routerFact, DataSource dataSource) throws SQLException {
        CallbackFactCtxVO fact = getFact();
        boolean isAdded = false;
        if (dsId != null && dataSource != null && fact != null) {
            String key = getFactKey(dsId, routerFact);
            Map<String, CallbackSessionFact> sessions = fact.getSessions();
            if (!sessions.containsKey(key)) {
                SqlSession sqlSession = fact.getSqlSessionProxy();
                if (fact.isBatch()) {
                    //sqlSession.startBatch();
                }
                CallbackSessionFact sessionFact = new CallbackFactCtxVO.CallbackSessionFact();
                sessionFact.setDataSource(dataSource);
                sessionFact.setSqlSessionProxy(sqlSession);

                sessions.put(key, sessionFact);
                isAdded = true;
            }
        }
        return isAdded;
    }

    public static void setConnection(String dsId, RouterFactCtxVO routerFact, Connection connection) throws SQLException {
        CallbackFactCtxVO fact = getFact();
        if (dsId != null && connection != null && fact != null) {
            String key = getFactKey(dsId, routerFact);
            Map<String, CallbackSessionFact> sessions = fact.getSessions();
            if (sessions.containsKey(key)) {
                CallbackSessionFact sessionFact = sessions.get(key);
                sessionFact.setConnection(connection);
            }
        }
    }

    public static List<SqlSession> getAllSqlMapSessions() {
        CallbackFactCtxVO fact = getFact();
        List<SqlSession> sqlMapSessions = new ArrayList<>();
        if (fact != null) {
            Map<String, CallbackSessionFact> sessions = fact.getSessions();
            for (CallbackSessionFact sessionFact : sessions.values()) {
                sqlMapSessions.add(sessionFact.getSqlSessionProxy());
            }
        }
        return sqlMapSessions;
    }

    public static List<Connection> getAllConnections() {
        CallbackFactCtxVO fact = getFact();
        List<Connection> connections = new ArrayList<Connection>();
        if (fact != null) {
            Map<String, CallbackSessionFact> sessions = fact.getSessions();
            for (CallbackSessionFact sessionFact : sessions.values()) {
                connections.add(sessionFact.getConnection());
            }
        }
        return connections;
    }
}
