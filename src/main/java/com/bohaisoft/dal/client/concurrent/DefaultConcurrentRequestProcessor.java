/**
 * Copyright(c) 2012 yihaodian. All rights reserved.
 * dal-client-1.1.0
 */
package com.bohaisoft.dal.client.concurrent;


import com.bohaisoft.dal.client.router.config.dataobject.ShardingMapping;
import com.bohaisoft.dal.client.router.support.RouterFactCtx;
import com.bohaisoft.dal.client.router.support.RouterFactCtxVO;
import com.bohaisoft.dal.client.spring.DalSqlSessionTemplate;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 默认并发请求处理类
 *
 * @author wuxiang
 * @since 2013-1-7
 */
public class DefaultConcurrentRequestProcessor implements IConcurrentRequestProcessor {

    static final Logger logger = LoggerFactory.getLogger(DefaultConcurrentRequestProcessor.class);

    private SqlSession sqlSessionProxy;

    long concurrentExecuteTimeout;

    public DefaultConcurrentRequestProcessor() {
    }

    public DefaultConcurrentRequestProcessor(SqlSession sqlSessionProxy) {
        this.sqlSessionProxy = sqlSessionProxy;
    }

    @Override
    public <E> List<E> process(SqlSession sqlSession, List<ConcurrentRequest> requests, final boolean earlyReleaseConnection) {
        List<E> resultList = new ArrayList<>();

        if (CollectionUtils.isEmpty(requests))
            return resultList;

        ConcurrentRequestHolder requestHolder = new ConcurrentRequestHolder();
        List<ConcurrentRequestWrapper> requestWrappers = toRequestWrappers(sqlSession, requestHolder, requests);
        final CountDownLatch latch = new CountDownLatch(requestWrappers.size());
        List<Future<Object>> futures = new ArrayList<Future<Object>>();

        for (ConcurrentRequestWrapper reqWrapper : requestWrappers) {
            final ConcurrentRequestWrapper innerReqWrapper = reqWrapper;

            ConcurrentRequest request = reqWrapper.getOriginalRequest();
            futures.add(request.getExecutor().submit(new Callable<Object>() {
                public Object call() throws Exception {
                    try {
                        return executeWith(sqlSession, innerReqWrapper, earlyReleaseConnection);
                    } finally {
                        latch.countDown();
                    }
                }
            }));
        }

        if (concurrentExecuteTimeout > 0) {
            try {
                latch.await(concurrentExecuteTimeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new ConcurrencyFailureException("interrupted when processing data access request in concurrency", e);
            } finally {
                if (earlyReleaseConnection)
                    requestHolder.releaseAllRequests();
            }
        } else {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new ConcurrencyFailureException("interrupted when processing data access request in concurrency", e);
            } finally {
                if (earlyReleaseConnection)
                    requestHolder.releaseAllRequests();
            }
        }

        fillResultListWithFutureResults(futures, (List<Object>) resultList);

        return resultList;
    }

    protected Object executeWith(SqlSession sqlSession, ConcurrentRequestWrapper reqWrapper, boolean earlyReleaseConnection) {
        ConcurrentRequest request = reqWrapper.getOriginalRequest();
        final DefaultSqlMapClientCallback action = request.getAction();
        final Connection connection = reqWrapper.getConnection();
        final String statementName = request.getStatementName();
        final List<ShardingMapping> shardingMappingSet = request.getShardingMappingSet();
        final Object parameters = request.getParameters();

        //SqlMapSession session = getSqlMapClient().openSession();

        try {
            RouterFactCtxVO fact = new RouterFactCtxVO();
            fact.setVirtualTablesMapping(shardingMappingSet);
            fact.setParameters(parameters);
            fact.setStatementName(statementName);
            RouterFactCtx.setRfvoholder(fact);
            try {
                return action.doInSqlMapClient(sqlSessionProxy);
            } catch (SQLException ex) {
                throw new SQLErrorCodeSQLExceptionTranslator().translate("SqlMapClient operation", null, ex);
            }
        } finally {
            RouterFactCtx.clearRfvoholder();
            sqlSessionProxy.close();
            if (earlyReleaseConnection)
                reqWrapper.releaseConnection();
        }
    }

    private <E> void fillResultListWithFutureResults(List<Future<E>> futures, List<E> resultList) {
        for (Future<E> future : futures) {
            try {
                E result = future.get();
                if (result instanceof List) {
                    resultList.addAll((List) result);
                } else {
                    resultList.add(result);
                }
            } catch (InterruptedException e) {
                throw new ConcurrencyFailureException("interrupted when processing data access request in concurrency", e);
            } catch (ExecutionException e) {
                throw new ConcurrencyFailureException("something goes wrong in processing", e);
            }
        }
    }

    private List<ConcurrentRequestWrapper> toRequestWrappers(SqlSession sqlSession, ConcurrentRequestHolder requestHolder, List<ConcurrentRequest> requests) {
        List<ConcurrentRequestWrapper> reqWrappers = new ArrayList<ConcurrentRequestWrapper>();
        for (ConcurrentRequest request : requests) {
            DataSource dataSource = request.getDataSource();
            boolean transactionAware = (dataSource instanceof TransactionAwareDataSourceProxy);
            Connection springCon = null;
            try {
                springCon = (transactionAware ? dataSource.getConnection() : DataSourceUtils.doGetConnection(dataSource));
            } catch (SQLException ex) {
                throw new CannotGetJdbcConnectionException("Could not get JDBC Connection", ex);
            }
            DalSqlSessionTemplate sqlSessionTemplate = (DalSqlSessionTemplate) sqlSession;
            Environment environment = sqlSessionTemplate.getConfiguration().getEnvironment();
            SqlSessionFactory sqlSessionFactory = sqlSessionTemplate.getSqlSessionFactory();
            sqlSessionFactory.getConfiguration().setEnvironment(new Environment.Builder(environment.getId()).
                    dataSource(dataSource).transactionFactory(environment.getTransactionFactory()).build());

            ConcurrentRequestWrapper reqWrapper = new ConcurrentRequestWrapper();
            reqWrapper.setRequestHolder(requestHolder);
            reqWrapper.setOriginalRequest(request);
            reqWrapper.setConnection(springCon);
            reqWrapper.setTransactionAware(transactionAware);
            reqWrappers.add(reqWrapper);
        }

        return reqWrappers;
    }

    public SqlSession getSqlSessionProxy() {
        return sqlSessionProxy;
    }

    public void setSqlSessionProxy(SqlSession sqlSessionProxy) {
        this.sqlSessionProxy = sqlSessionProxy;
    }

    /*
         * (non-Javadoc)
         *
         * @see com.yihaodian.ydal.client.concurrent.IConcurrentRequestProcessor#
         * setConcurrentTimeout(int)
         */
    @Override
    public void setConcurrentExecuteTimeout(long concurrentExecuteTimeout) {
        this.concurrentExecuteTimeout = concurrentExecuteTimeout;
    }

}
