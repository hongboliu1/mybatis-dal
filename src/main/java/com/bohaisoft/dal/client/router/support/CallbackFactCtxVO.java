package com.bohaisoft.dal.client.router.support;

import org.apache.ibatis.session.SqlSession;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;


public class CallbackFactCtxVO {

    //是否是批量
    private boolean batch = false;

    private SqlSession sqlSessionProxy;

    public SqlSession getSqlSessionProxy() {
        return sqlSessionProxy;
    }

    public void setSqlSessionProxy(SqlSession sqlSessionProxy) {
        this.sqlSessionProxy = sqlSessionProxy;
    }

    //数据源对应的session映射,key为数据源id+table
    private Map<String, CallbackSessionFact> sessions = new HashMap<String, CallbackSessionFact>();

    public Map<String, CallbackSessionFact> getSessions() {
        return sessions;
    }

    public void setSessions(Map<String, CallbackSessionFact> sessions) {
        this.sessions = sessions;
    }

    public boolean isBatch() {
        return batch;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    public SqlSession getCurrentSession(String dsId, RouterFactCtxVO routerFact) {
        if (dsId != null) {
            String key = CallbackFactCtx.getFactKey(dsId, routerFact);
            CallbackSessionFact sessionFact = sessions.get(key);
            if (sessionFact != null) {
                return sessionFact.getSqlSessionProxy();
            }
        }
        return null;
    }

    static class CallbackSessionFact {
        private SqlSession sqlSessionProxy;
        private DataSource dataSource;
        private Connection connection;

        public Connection getConnection() {
            return connection;
        }

        public void setConnection(Connection connection) {
            this.connection = connection;
        }

        public DataSource getDataSource() {
            return dataSource;
        }

        public void setDataSource(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public SqlSession getSqlSessionProxy() {
            return sqlSessionProxy;
        }

        public void setSqlSessionProxy(SqlSession sqlSessionProxy) {
            this.sqlSessionProxy = sqlSessionProxy;
        }
    }


}
