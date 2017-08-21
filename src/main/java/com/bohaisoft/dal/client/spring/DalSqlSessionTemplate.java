package com.bohaisoft.dal.client.spring;

import com.bohaisoft.dal.client.cache.CacheFactory;
import com.bohaisoft.dal.client.cache.ICache;
import com.bohaisoft.dal.client.concurrent.*;
import com.bohaisoft.dal.client.datasource.AtomDS;
import com.bohaisoft.dal.client.datasource.GroupDS;
import com.bohaisoft.dal.client.datasource.MasterDS;
import com.bohaisoft.dal.client.datasource.MatrixDS;
import com.bohaisoft.dal.client.datasource.ha.ExtSQLStateSQLExceptionTranslator;
import com.bohaisoft.dal.client.exception.ConnectionPoolOverException;
import com.bohaisoft.dal.client.exception.NoDatasourceFoundException;
import com.bohaisoft.dal.client.exception.UncategorizedClientException;
import com.bohaisoft.dal.client.merger.IMerger;
import com.bohaisoft.dal.client.merger.IMergerFilter;
import com.bohaisoft.dal.client.merger.MergerUtils;
import com.bohaisoft.dal.client.router.RoutingFacade;
import com.bohaisoft.dal.client.router.config.dataobject.ShardingFactDO;
import com.bohaisoft.dal.client.router.config.dataobject.ShardingMapping;
import com.bohaisoft.dal.client.router.support.*;
import com.bohaisoft.dal.client.router.support.mybatis.SqlMapExtensible;
import com.bohaisoft.dal.client.router.support.mybatis.version3_2_7.SqlMapExtensibleImpl;
import com.bohaisoft.dal.client.support.parser.SQLEngine;
import com.bohaisoft.dal.client.transaction.support.TransactionUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.session.*;
import org.mybatis.spring.MyBatisExceptionTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.jdbc.support.SQLStateSQLExceptionTranslator;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.reflect.Proxy.newProxyInstance;
import static org.apache.ibatis.reflection.ExceptionUtil.unwrapThrowable;
import static org.mybatis.spring.SqlSessionUtils.*;
import static org.springframework.util.Assert.notNull;

/**
 * Created by liuhb on 2017/1/5.
 */
public class DalSqlSessionTemplate implements SqlSession, InitializingBean {

    static final Logger logger = LoggerFactory.getLogger(DalSqlSessionTemplate.class);

    private SqlSessionFactory sqlSessionFactory;

    private ExecutorType executorType;

    private final SqlSession sqlSessionProxy;

    private final PersistenceExceptionTranslator exceptionTranslator;

    private static Map<String, Integer> requestCount = new HashMap<>();

    private Map<String, IMerger<Object, Object>> mergers = new HashMap<>();

    private RoutingFacade dalclientService;

    private SqlMapExtensible sqlMapExtensible;

    private SQLStateSQLExceptionTranslator sqlExTranslator = new ExtSQLStateSQLExceptionTranslator();
    // unused
    private boolean needLogSql = false;
    // unused
    private boolean needTimeOut = false;
    // unused
    private Integer executeTimeout = 15;

    private boolean profileLongTimeRunningSql = true;

    private long longTimeRunningSqlIntervalThreshold = 5000;

    private int calStatInterval = 60000;

    private boolean calSwitch = true;

    private int auditQueueSize = 60;

    private int auditQueryResultsLimit = 2000;

    private boolean auditSqlExplain = false;

    private boolean logSqlExplainDetail = false;

    private boolean sqlTableCacheEnable = true;

    private int sqlTableCacheSize = 9999;

    private List<String> sqlTableCacheExclusions = null;

    private boolean mergeShardingQueriesInSameDataSource = true;

    private boolean executeQueryInConcurrency = true;

    private boolean executeInsertInConcurrency = true;

    private boolean executeUpdateInConcurrency = false;

    private boolean executeDeleteInConcurrency = false;

    private IConcurrentRequestProcessor concurrentRequestProcessor;

    private Map<String, ExecutorService> dataSourceSpecificExecutors = new HashMap<>();

    private List<ExecutorService> internalExecutorServiceRegistry = new ArrayList<>();

    private ICache sqlCache = null;

    private long concurrentExecuteTimeout = 0;

    private boolean earlyReleaseConnection = true;

    private boolean onlyErrorsLogSwitch = true;

    /**
     * Constructs a Spring managed SqlSession with the {@code SqlSessionFactory}
     * provided as an argument.
     *
     * @param sqlSessionFactory
     */
    public DalSqlSessionTemplate(SqlSessionFactory sqlSessionFactory) {
        this(sqlSessionFactory, sqlSessionFactory.getConfiguration().getDefaultExecutorType());
    }

    /**
     * Constructs a Spring managed SqlSession with the {@code SqlSessionFactory}
     * provided as an argument and the given {@code ExecutorType}
     * {@code ExecutorType} cannot be changed once the {@code DalSqlSessionTemplate}
     * is constructed.
     *
     * @param sqlSessionFactory
     * @param executorType
     */
    public DalSqlSessionTemplate(SqlSessionFactory sqlSessionFactory, ExecutorType executorType) {
        this(sqlSessionFactory, executorType,
                new MyBatisExceptionTranslator(
                        sqlSessionFactory.getConfiguration().getEnvironment().getDataSource(), true));
    }

    /**
     * Constructs a Spring managed {@code SqlSession} with the given
     * {@code SqlSessionFactory} and {@code ExecutorType}.
     * A custom {@code SQLExceptionTranslator} can be provided as an
     * argument so any {@code PersistenceException} thrown by MyBatis
     * can be custom translated to a {@code RuntimeException}
     * The {@code SQLExceptionTranslator} can also be null and thus no
     * exception translation will be done and MyBatis exceptions will be
     * thrown
     *
     * @param sqlSessionFactory
     * @param executorType
     * @param exceptionTranslator
     */
    public DalSqlSessionTemplate(SqlSessionFactory sqlSessionFactory, ExecutorType executorType,
                                 PersistenceExceptionTranslator exceptionTranslator) {

        notNull(sqlSessionFactory, "Property 'sqlSessionFactory' is required");
        notNull(executorType, "Property 'executorType' is required");

        this.sqlSessionFactory = sqlSessionFactory;
        this.executorType = executorType;
        this.exceptionTranslator = exceptionTranslator;
        this.sqlSessionProxy = (SqlSession) newProxyInstance(
                SqlSessionFactory.class.getClassLoader(),
                new Class[]{SqlSession.class},
                new DalSqlSessionInterceptor());
    }

    public SqlSessionFactory getSqlSessionFactory() {
        return this.sqlSessionFactory;
    }

    public void setSqlSessionFactory(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
    }

    public ExecutorType getExecutorType() {
        return this.executorType;
    }

    public PersistenceExceptionTranslator getPersistenceExceptionTranslator() {
        return this.exceptionTranslator;
    }

    /**
     * {@inheritDoc}
     */
    public <T> T selectOne(String statement) {
        return this.<T>selectOne(statement, null);
    }

    /**
     * {@inheritDoc}
     */
    public <T> T selectOne(String statement, Object parameter) {
        try {
            Collection<String> tableNames = SQLEngine.getInstance().getTableNamesByStatementName(this.sqlSessionFactory.getConfiguration(),
                    statement, parameter, this.sqlTableCacheEnable, this.sqlTableCacheExclusions);

            Map<AtomDS, List<ShardingMapping>> dataSources = lookupDataSourcesByRouter(SqlCommandType.SELECT, statement, parameter, tableNames);
            if (isPartitioningBehaviorEnabled()) {
                if (MapUtils.isEmpty(dataSources)) {
                    throw new NoDatasourceFoundException("No datasource found when execute:" + statement);
                }

                DefaultSqlMapClientCallback action;
                action = new DefaultSqlMapClientCallback() {
                    public Object doInSqlMapClient(SqlSession sqlSessionProxy) throws SQLException {
                        return sqlSessionProxy.<T>selectOne(statement, this.getParameterObject());
                    }
                };

                action.setInitialParameterObject(parameter);

                if (!isShardingOperation(SqlCommandType.SELECT, dataSources)) {
                    return (T) executeWith(statement, dataSources, action);
                } else {
                    List<Object> resultList = executeInConcurrency(SqlCommandType.SELECT, statement, dataSources, action);
                    Collection<T> filteredResultList = MergerUtils.select(resultList, new IMergerFilter() {
                        public boolean evaluate(final Object item) {
                            return item != null;
                        }
                    });
                    if (filteredResultList.size() > 1) {
                        throw new IncorrectResultSizeDataAccessException(1);
                    }
                    if (CollectionUtils.isEmpty(filteredResultList)) {
                        return null;
                    }
                    return filteredResultList.iterator().next();
                }
            } else {
                return sqlSessionProxy.selectOne(statement, parameter);
            }

        } catch (RuntimeException re) {
            throw re;
        } finally {
            RouterFactCtx.clearRfvoholder();
        }
    }

    private void lookupDataSourceBySelect(String statementName, Object parameterObject) {
        try {
            //com.bohaisoft.dal.mapper.getUserById
            Collection<String> tableNames = SQLEngine.getInstance().getTableNamesByStatementName(this.sqlSessionFactory.getConfiguration(),
                    statementName, parameterObject, this.sqlTableCacheEnable, this.sqlTableCacheExclusions);

            Map<AtomDS, List<ShardingMapping>> dataSources = lookupDataSourcesByRouter(SqlCommandType.SELECT, statementName, parameterObject, tableNames);
            if (isPartitioningBehaviorEnabled()) {
                if (MapUtils.isEmpty(dataSources)) {
                    throw new NoDatasourceFoundException("No datasource found when execute:" + statementName);
                }
                if (!isShardingOperation(SqlCommandType.SELECT, dataSources)) {
                    AtomDS dataSource = dataSources.entrySet().iterator().next().getKey();
                    final DataSource targetDataSource = dataSource.getTargetDataSource();
                    SqlSessionFactory newSqlSessionFactory = this.sqlSessionFactory;
                    Environment environment = newSqlSessionFactory.getConfiguration().getEnvironment();
                    newSqlSessionFactory.getConfiguration().setEnvironment(new Environment.Builder(environment.getId()).
                            dataSource(targetDataSource).transactionFactory(environment.getTransactionFactory()).build());
                    this.sqlSessionFactory = newSqlSessionFactory;
                }
            }
        } catch (RuntimeException e) {
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    public <K, V> Map<K, V> selectMap(String statement, String mapKey) {
        return this.<K, V>selectMap(statement, null, mapKey, RowBounds.DEFAULT);
    }

    /**
     * {@inheritDoc}
     */
    public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey) {
        return this.<K, V>selectMap(statement, parameter, mapKey, RowBounds.DEFAULT);
    }

    /**
     * {@inheritDoc}
     */
    public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey, RowBounds rowBounds) {
        try {
            //com.bohaisoft.dal.mapper.getUserById
            Collection<String> tableNames = SQLEngine.getInstance().getTableNamesByStatementName(this.sqlSessionFactory.getConfiguration(),
                    statement, parameter, this.sqlTableCacheEnable, this.sqlTableCacheExclusions);

            Map<AtomDS, List<ShardingMapping>> dataSources = lookupDataSourcesByRouter(SqlCommandType.SELECT, statement, parameter, tableNames);
            if (isPartitioningBehaviorEnabled()) {
                if (MapUtils.isEmpty(dataSources)) {
                    throw new NoDatasourceFoundException("No datasource found when execute:" + statement);
                }

                DefaultSqlMapClientCallback action = null;
                action = new DefaultSqlMapClientCallback() {
                    public Object doInSqlMapClient(SqlSession sqlSessionProxy) throws SQLException {
                        return sqlSessionProxy.<K, V>selectMap(statement, this.getParameterObject(), mapKey, rowBounds);
                    }
                };
                action.setInitialParameterObject(parameter);

                Map<K, V> results = null;
                if (!isShardingOperation(SqlCommandType.SELECT, dataSources)) {
                    results = (Map<K, V>) executeWith(statement, dataSources, action);
                } else if (!this.isExecuteQueryInConcurrency(parameter)) {
                    results = new HashMap<>();
                    for (AtomDS atomDS : dataSources.keySet()) {
                        List<ShardingMapping> mappings = dataSources.get(atomDS);
                        for (ShardingMapping mapping : mappings) {
                            setShardingParameters(parameter, mapping);
                            results.putAll((Map<K, V>) executeWith(statement, atomDS, action));
                        }
                    }
                } else {
                    List<Object> originalResults = executeInConcurrency(SqlCommandType.SELECT, statement, dataSources, action);
                    results = new HashMap<>();
                    for (Object item : originalResults) {
                        results.putAll((Map) item);
                    }
                    if (results != null) {
                        int resultSize = results.size();
                        if (resultSize > auditQueryResultsLimit) {
                            logger.warn("The query '" + statement + "' returns too many results:" + resultSize + ",limit:" + auditQueryResultsLimit);
                        }
                    }
                }
                return results;
            } else {
                return sqlSessionProxy.selectMap(statement, parameter, mapKey, rowBounds);
            }
        } catch (RuntimeException e) {
            throw e;
        } finally {
            RouterFactCtx.clearRfvoholder();
        }
    }

    /**
     * {@inheritDoc}
     */
    public <E> List<E> selectList(String statement) {
        return this.<E>selectList(statement, null);
    }


    /**
     * {@inheritDoc}
     */
    public <E> List<E> selectList(String statement, Object parameter, RowBounds rowBounds) {
        try {
            //com.bohaisoft.dal.mapper.getUserById
            Collection<String> tableNames = SQLEngine.getInstance().getTableNamesByStatementName(this.sqlSessionFactory.getConfiguration(),
                    statement, parameter, this.sqlTableCacheEnable, this.sqlTableCacheExclusions);

            Map<AtomDS, List<ShardingMapping>> dataSources = lookupDataSourcesByRouter(SqlCommandType.SELECT, statement, parameter, tableNames);
            if (isPartitioningBehaviorEnabled()) {
                if (MapUtils.isEmpty(dataSources)) {
                    throw new NoDatasourceFoundException("No datasource found when execute:" + statement);
                }

                DefaultSqlMapClientCallback action = null;
                action = new DefaultSqlMapClientCallback() {
                    public Object doInSqlMapClient(SqlSession sqlSessionProxy) throws SQLException {
                        return sqlSessionProxy.<E>selectList(statement, this.getParameterObject(), rowBounds);
                    }
                };
                action.setInitialParameterObject(parameter);

                List<E> results = null;
                if (!isShardingOperation(SqlCommandType.SELECT, dataSources)) {
                    results = (List) executeWith(statement, dataSources, action);
                } else if (!this.isExecuteQueryInConcurrency(parameter)) {
                    results = new ArrayList();
                    for (AtomDS atomDS : dataSources.keySet()) {
                        List<ShardingMapping> mappings = dataSources.get(atomDS);
                        for (ShardingMapping mapping : mappings) {
                            setShardingParameters(parameter, mapping);
                            results.addAll((List) executeWith(statement, atomDS, action));
                        }
                    }
                } else {
                    results = executeInConcurrency(SqlCommandType.SELECT, statement, dataSources, action);
                }
                return results;
            } else {
                return sqlSessionProxy.selectList(statement, parameter, rowBounds);
            }
        } catch (RuntimeException e) {
            throw e;
        } finally {
            RouterFactCtx.clearRfvoholder();
        }
    }

    /**
     * {@inheritDoc}
     */
    public <E> List<E> selectList(String statement, Object parameter) {
        return this.<E>selectList(statement, parameter, RowBounds.DEFAULT);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <E> List<E> executeInConcurrency(SqlCommandType statementType, String statementName, Map<AtomDS, List<ShardingMapping>> dataSources,
                                            DefaultSqlMapClientCallback action) {
        List<ConcurrentRequest> requests = new ArrayList<>();

        for (AtomDS ds : dataSources.keySet()) {
            ExecutorService executor = getDataSourceSpecificExecutors().get(ds.getId());
            if (executor == null) {
                throw new IllegalArgumentException("No executor service found for datasource:" + ds.getId());
            }
            List<ShardingMapping> shardingMappings = dataSources.get(ds);
            if (statementType.equals(SqlCommandType.SELECT) && this.isMergeShardingQueriesInSameDataSource()) {
                ConcurrentRequest request = new ConcurrentRequest();
                request.setStatementName(statementName);
                request.setAction(action);
                request.setDataSource(ds.getTargetDataSource());
                request.setExecutor(executor);
                request.setShardingMappingSet(shardingMappings);
                Object initialParameter = action.getInitialParameterObject();
                if (initialParameter != null) {
                    if (initialParameter.getClass().isArray()) {
                        Object[] parameters = (Object[]) initialParameter;
                        if (parameters.length > 0 && !org.springframework.util.CollectionUtils.isEmpty(shardingMappings)) {
                            Object[] newParameters = new Object[parameters.length];
                            int pos = 0;
                            for (ShardingMapping mapping : shardingMappings) {
                                if (mapping.getParameterObjects() != null) {
                                    for (Object param : mapping.getParameterObjects()) {
                                        newParameters[pos++] = param;
                                    }
                                }
                            }
                            if (newParameters.length == 0) {
                                continue;
                            }
                            request.setParameters(newParameters);
                        }
                    } else if (initialParameter instanceof Collection) {
                        Collection parameters = (Collection) initialParameter;
                        if (!parameters.isEmpty() && CollectionUtils.isNotEmpty(shardingMappings)) {
                            List<Object> newParameters = new ArrayList<Object>();
                            for (ShardingMapping mapping : shardingMappings) {
                                if (mapping.getParameterObjects() != null) {
                                    newParameters.addAll(mapping.getParameterObjects());
                                }
                            }
                            if (newParameters.isEmpty()) {
                                continue;
                            }
                            request.setParameters(newParameters);
                        }
                    } else if (initialParameter instanceof Map) {
                        Map parameters = (Map) initialParameter;
                        if (!parameters.isEmpty() && !org.springframework.util.CollectionUtils.isEmpty(shardingMappings)) {
                            Object list = parameters.get("list");
                            if (list instanceof Collection || list != null && list.getClass().isArray()) {
                                int splitSize = 0;
                                for (ShardingMapping mapping : shardingMappings) {
                                    if (mapping.getParameterObjects() != null) {
                                        ConcurrentRequest req = new ConcurrentRequest();
                                        req.setStatementName(statementName);
                                        req.setAction(action);
                                        req.setDataSource(ds.getTargetDataSource());
                                        req.setExecutor(executor);
                                        List<ShardingMapping> smList = new ArrayList<ShardingMapping>();
                                        smList.add(mapping);
                                        req.setShardingMappingSet(smList);
                                        Map newParameters = new HashMap(parameters);
                                        newParameters.put("list", mapping.getParameterObjects());
                                        req.setParameters(newParameters);
                                        requests.add(req);
                                        splitSize++;
                                    }
                                }
                                if (splitSize > 0) {
                                    continue;
                                }
                            }
                        }
                    }
                }
                requests.add(request);
            } else if (statementType.equals(SqlCommandType.INSERT)) {
                if (shardingMappings != null) {
                    for (ShardingMapping shardingMapping : shardingMappings) {
                        List<Object> params = shardingMapping.getParameterObjects();
                        if (params != null) {
                            ConcurrentRequest request = new ConcurrentRequest();
                            request.setStatementName(statementName);
                            request.setAction(action);
                            request.setDataSource(ds.getTargetDataSource());
                            request.setExecutor(executor);
                            List<ShardingMapping> set = new ArrayList<ShardingMapping>();
                            set.add(shardingMapping);
                            request.setShardingMappingSet(set);
                            request.setParameters(params);
                            requests.add(request);
                        }
                    }
                }
            } else {
                if (shardingMappings != null) {
                    for (ShardingMapping shardingMapping : shardingMappings) {
                        ConcurrentRequest request = new ConcurrentRequest();
                        request.setStatementName(statementName);
                        request.setAction(action);
                        request.setDataSource(ds.getTargetDataSource());
                        request.setExecutor(executor);
                        List<ShardingMapping> set = new ArrayList<ShardingMapping>();
                        set.add(shardingMapping);
                        request.setShardingMappingSet(set);
                        requests.add(request);
                    }
                }
            }
            TransactionUtil.beginAtomDSTransaction(ds);
        }

        List<E> results = getConcurrentRequestProcessor().process(this, requests, earlyReleaseConnection);
        return results;
    }


    protected Object executeWith(String statementName, Map<AtomDS, List<ShardingMapping>> dataSources, final SqlMapClientCallback action) {
        AtomDS dataSource = dataSources.entrySet().iterator().next().getKey();
        return executeWith(statementName, dataSource, action);
    }

    protected Object executeWith(String statementName, AtomDS dataSource, final SqlMapClientCallback action) {
        TransactionUtil.beginAtomDSTransaction(dataSource);

        boolean allow = true;
        final String dsId = dataSource.getTargetId();

        synchronized (requestCount) {
            int maxNum = dataSource.getMaxRequest();
            if (requestCount.get(dsId) == null) {
                allow = true;
            } else if (requestCount.get(dsId) > maxNum) {
                allow = false;
            }
        }
        if (allow) {
            boolean isGetConnection = true;
            boolean isCallback = false;
            final DataSource targetDataSource = dataSource.getTargetDataSource();
            final CallbackFactCtxVO callbackFact = CallbackFactCtx.getFact();
            final RouterFactCtxVO routerFact = RouterFactCtx.getRfvoholder();
            if (routerFact != null) {
                routerFact.setStatementName(statementName);
            }
            try {
                synchronized (requestCount) {
                    if (requestCount.get(dsId) != null) {
                        requestCount.put(dsId, requestCount.get(dsId) + 1);
                    } else {
                        requestCount.put(dsId, 1);
                    }
                }
                if (callbackFact != null) {
                    isCallback = true;
                    isGetConnection = CallbackFactCtx.addDataSourceIfAbsent(dsId, routerFact, targetDataSource);
                }
                boolean transactionAware = (targetDataSource instanceof TransactionAwareDataSourceProxy);
                Connection springCon = null;
                if (isGetConnection) {
                    try {
                        springCon = (transactionAware ? targetDataSource.getConnection() : DataSourceUtils.doGetConnection(targetDataSource));
                    } catch (SQLException ex) {
                        throw new CannotGetJdbcConnectionException("Could not get JDBC Connection", ex);
                    }
                }
                try {
                    RouterFactCtx.setRfvoholder(routerFact);

                    Configuration configuration = this.sqlSessionFactory.getConfiguration();
                    Environment environment = configuration.getEnvironment();
                    configuration.setEnvironment(new Environment.Builder(environment.getId()).
                            dataSource(dataSource.getTargetDataSource()).transactionFactory(environment.getTransactionFactory()).build());

                    Object result = action.doInSqlMapClient(this.sqlSessionProxy);
                    return result;


                } catch (SQLException ex) {
                    logger.error(ex.getMessage(), ex);
                    throw new SQLErrorCodeSQLExceptionTranslator().translate("SqlMapClient operation", null, ex);
                } finally {
                    RouterFactCtx.clearRfvoholder();
                    try {
                        if (earlyReleaseConnection && springCon != null && !isCallback) {
                            if (transactionAware) {
                                springCon.close();
                            } else {
                                DataSourceUtils.doReleaseConnection(springCon, targetDataSource);
                            }
                        }
                    } catch (Throwable ex) {
                        logger.info("Could not close JDBC Connection", ex);
                    }
                }
            } catch (DataAccessException t) {
                throw t;
            } catch (Throwable t) {
                throw new UncategorizedClientException("unknown exception when performing data access operation,router fact:" + routerFact, t);
            } finally {
                synchronized (requestCount) {
                    if (requestCount.get(dsId) != null) {
                        requestCount.put(dsId, requestCount.get(dsId) - 1);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Release:" + (requestCount.get(dsId)));
                    }
                }
            }
        } else {
            throw new ConnectionPoolOverException("Too many concurrency request on:" + dsId);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void select(String statement, ResultHandler handler) {
        this.select(statement, null, handler);
    }

    /**
     * {@inheritDoc}
     */
    public void select(String statement, Object parameter, ResultHandler handler) {
        this.select(statement, parameter, RowBounds.DEFAULT, handler);
    }

    /**
     * {@inheritDoc}
     */
    public void select(String statement, Object parameter, RowBounds rowBounds, ResultHandler handler) {
        try {
            Collection<String> tableNames = SQLEngine.getInstance().getTableNamesByStatementName(this.sqlSessionFactory.getConfiguration(),
                    statement, parameter, this.sqlTableCacheEnable, this.sqlTableCacheExclusions);

            Map<AtomDS, List<ShardingMapping>> dataSources = lookupDataSourcesByRouter(SqlCommandType.SELECT, statement, parameter, tableNames);
            if (isPartitioningBehaviorEnabled()) {
                if (MapUtils.isEmpty(dataSources)) {
                    throw new NoDatasourceFoundException("No datasource found when execute:" + statement);
                }

                DefaultSqlMapClientCallback action = null;
                action = new DefaultSqlMapClientCallback() {
                    public Object doInSqlMapClient(SqlSession sqlSessionProxy) throws SQLException {
                        sqlSessionProxy.select(statement, this.getParameterObject(), rowBounds, handler);
                        return null;
                    }
                };
                action.setInitialParameterObject(parameter);
                if (!isShardingOperation(SqlCommandType.SELECT, dataSources)) {
                    executeWith(statement, dataSources, action);
                } else if (!this.isExecuteQueryInConcurrency(parameter)) {
                    for (AtomDS atomDS : dataSources.keySet()) {
                        List<ShardingMapping> mappings = dataSources.get(atomDS);
                        for (ShardingMapping mapping : mappings) {
                            setShardingParameters(parameter, mapping);
                            executeWith(statement, atomDS, action);
                        }
                    }
                } else {
                    executeInConcurrency(SqlCommandType.SELECT, statement, dataSources, action);
                }
            } else {
                sqlSessionProxy.select(statement, parameter, rowBounds, handler);
            }
        } catch (RuntimeException re) {
            throw re;
        }
    }

    /**
     * {@inheritDoc}
     */
    public int insert(String statement) {
        return this.insert(statement, null);
    }

    /**
     * {@inheritDoc}
     */
    public int insert(String statement, Object parameter) {
        try {
            //com.bohaisoft.dal.mapper.getUserById
            Collection<String> tableNames = SQLEngine.getInstance().getTableNamesByStatementName(this.sqlSessionFactory.getConfiguration(),
                    statement, parameter, this.sqlTableCacheEnable, this.sqlTableCacheExclusions);

            Map<AtomDS, List<ShardingMapping>> dataSources = lookupDataSourcesByRouter(SqlCommandType.INSERT, statement, parameter, tableNames);
            if (isPartitioningBehaviorEnabled()) {
                if (MapUtils.isEmpty(dataSources)) {
                    throw new NoDatasourceFoundException("No datasource found when execute:" + statement);
                }
                DefaultSqlMapClientCallback action = new DefaultSqlMapClientCallback() {
                    public Object doInSqlMapClient(SqlSession sqlSessionProxy) throws SQLException {
                        Object parameterObject = this.getParameterObject();
                        if (parameterObject instanceof Collection) {
                            List<Object> results = new ArrayList<Object>();
                            Iterator ir = ((Collection) parameterObject).iterator();
                            while (ir.hasNext()) {
                                Object parameter = ir.next();
                                results.add(sqlSessionProxy.insert(statement, parameter));
                            }
                            return results;
                        } else {
                            return sqlSessionProxy.insert(statement, parameterObject);
                        }
                    }
                };
                action.setInitialParameterObject(parameter);
                int results = 0;
                if (!isShardingOperation(SqlCommandType.INSERT, dataSources)) {
                    results = (int) executeWith(statement, dataSources, action);
                } else if (!this.isExecuteQueryInConcurrency(parameter)) {
                    for (AtomDS atomDS : dataSources.keySet()) {
                        List<ShardingMapping> mappings = dataSources.get(atomDS);
                        for (ShardingMapping mapping : mappings) {
                            setShardingParameters(parameter, mapping);
                            results += (int) executeWith(statement, atomDS, action);
                        }
                    }
                } else {
                    List<Object> list = executeInConcurrency(SqlCommandType.INSERT, statement, dataSources, action);
                    for (Object item : list) {
                        results += (int) item;
                    }
                }
                return results;
            } else {
                return sqlSessionProxy.insert(statement, parameter);
            }
        } catch (RuntimeException e) {
            throw e;
        } finally {
            RouterFactCtx.clearRfvoholder();
        }
    }

    /**
     * {@inheritDoc}
     */
    public int update(String statement) {
        return this.update(statement, null);
    }

    /**
     * {@inheritDoc}
     */
    public int update(String statement, Object parameter) {
        try {
            //com.bohaisoft.dal.mapper.getUserById
            Collection<String> tableNames = SQLEngine.getInstance().getTableNamesByStatementName(this.sqlSessionFactory.getConfiguration(),
                    statement, parameter, this.sqlTableCacheEnable, this.sqlTableCacheExclusions);

            Map<AtomDS, List<ShardingMapping>> dataSources = lookupDataSourcesByRouter(SqlCommandType.UPDATE, statement, parameter, tableNames);
            if (isPartitioningBehaviorEnabled()) {
                if (MapUtils.isEmpty(dataSources)) {
                    throw new NoDatasourceFoundException("No datasource found when execute:" + statement);
                }
                DefaultSqlMapClientCallback action;
                action = new DefaultSqlMapClientCallback() {
                    public Object doInSqlMapClient(SqlSession sqlSessionProxy) throws SQLException {
                        return sqlSessionProxy.update(statement, this.getParameterObject());
                    }
                };
                action.setInitialParameterObject(parameter);
                int results = 0;
                if (!isShardingOperation(SqlCommandType.UPDATE, dataSources)) {
                    results = (int) executeWith(statement, dataSources, action);
                } else if (!this.isExecuteQueryInConcurrency(parameter)) {
                    for (AtomDS atomDS : dataSources.keySet()) {
                        List<ShardingMapping> mappings = dataSources.get(atomDS);
                        for (ShardingMapping mapping : mappings) {
                            setShardingParameters(parameter, mapping);
                            results += (int) executeWith(statement, atomDS, action);
                        }
                    }
                } else {
                    List<Object> list = executeInConcurrency(SqlCommandType.UPDATE, statement, dataSources, action);
                    for (Object item : list) {
                        results += (int) item;
                    }
                }
                return results;
            } else {
                return sqlSessionProxy.update(statement, parameter);
            }
        } catch (RuntimeException e) {
            throw e;
        } finally {
            RouterFactCtx.clearRfvoholder();
        }
    }

    /**
     * {@inheritDoc}
     */
    public int delete(String statement) {
        return this.sqlSessionProxy.delete(statement);
    }

    /**
     * {@inheritDoc}
     */
    public int delete(String statement, Object parameter) {
        return this.sqlSessionProxy.delete(statement, parameter);
    }

    /**
     * {@inheritDoc}
     */
    public <T> T getMapper(Class<T> type) {
        return getConfiguration().getMapper(type, this);
    }

    /**
     * {@inheritDoc}
     */
    public void commit() {
        throw new UnsupportedOperationException("Manual commit is not allowed over a Spring managed SqlSession");
    }

    /**
     * {@inheritDoc}
     */
    public void commit(boolean force) {
        throw new UnsupportedOperationException("Manual commit is not allowed over a Spring managed SqlSession");
    }

    /**
     * {@inheritDoc}
     */
    public void rollback() {
        throw new UnsupportedOperationException("Manual rollback is not allowed over a Spring managed SqlSession");
    }

    /**
     * {@inheritDoc}
     */
    public void rollback(boolean force) {
        throw new UnsupportedOperationException("Manual rollback is not allowed over a Spring managed SqlSession");
    }

    /**
     * {@inheritDoc}
     */
    public void close() {
        throw new UnsupportedOperationException("Manual close is not allowed over a Spring managed SqlSession");
    }

    /**
     * {@inheritDoc}
     */
    public void clearCache() {
        this.sqlSessionProxy.clearCache();
    }

    /**
     * {@inheritDoc}
     */
    public Configuration getConfiguration() {
        return this.sqlSessionFactory.getConfiguration();
    }

    /**
     * {@inheritDoc}
     */
    public Connection getConnection() {
        return this.sqlSessionProxy.getConnection();
    }

    /**
     * {@inheritDoc}
     *
     * @since 1.0.2
     */
    public List<BatchResult> flushStatements() {
        return this.sqlSessionProxy.flushStatements();
    }

    public boolean isProfileLongTimeRunningSql() {
        return profileLongTimeRunningSql;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (isProfileLongTimeRunningSql()) {
            if (longTimeRunningSqlIntervalThreshold <= 0) {
                throw new IllegalArgumentException("'longTimeRunningSqlIntervalThreshold' should have a positive value if 'profileLongTimeRunningSql' is set to true");
            }
        }
        if (auditQueryResultsLimit > 2000 || auditQueryResultsLimit < 100) {
            throw new IllegalArgumentException("'auditQueryResultsLimit' should between 100 and 2000");
        }

        if (sqlCache == null) {
            sqlCache = CacheFactory.INSTANCE.createCache(sqlTableCacheSize);
        }
        SQLEngine.getInstance().setSqlCache(sqlCache);


        concurrentRequestProcessor = new DefaultConcurrentRequestProcessor(this.sqlSessionProxy);

        concurrentRequestProcessor.setConcurrentExecuteTimeout(concurrentExecuteTimeout);

        createDefaultExecutorServices();
    }


    public boolean isExecuteQueryInConcurrency() {
        return executeQueryInConcurrency;
    }

    public void setExecuteQueryInConcurrency(boolean executeQueryInConcurrency) {
        this.executeQueryInConcurrency = executeQueryInConcurrency;
    }

    public boolean isExecuteInsertInConcurrency() {
        return executeInsertInConcurrency;
    }

    public void setExecuteInsertInConcurrency(boolean executeInsertInConcurrency) {
        this.executeInsertInConcurrency = executeInsertInConcurrency;
    }

    public boolean isExecuteUpdateInConcurrency() {
        return executeUpdateInConcurrency;
    }

    public void setExecuteUpdateInConcurrency(boolean executeUpdateInConcurrency) {
        this.executeUpdateInConcurrency = executeUpdateInConcurrency;
    }

    public boolean isExecuteDeleteInConcurrency() {
        return executeDeleteInConcurrency;
    }

    public void setExecuteDeleteInConcurrency(boolean executeDeleteInConcurrency) {
        this.executeDeleteInConcurrency = executeDeleteInConcurrency;
    }

    private void createDefaultExecutorServices() {
        if (this.executeQueryInConcurrency || this.executeInsertInConcurrency || this.executeUpdateInConcurrency || this.executeDeleteInConcurrency) {
            MatrixDS matrix = dalclientService.getMatrixDS();
            if (matrix != null) {
                Map<String, GroupDS> groups = matrix.getGroupMap();
                if (groups != null && groups.size() > 1) {
                    for (GroupDS group : groups.values()) {
                        MasterDS master = group.getMaster();
                        if (master != null && master.getMaster() != null) {
                            ExecutorService executor = createExecutorForSpecificDataSource(master.getMaster());
                            getDataSourceSpecificExecutors().put(master.getMaster().getId(), executor);
                        }
                        Map<String, AtomDS> slaves = group.getMapToSlaveDS();
                        if (slaves != null) {
                            for (AtomDS slave : slaves.values()) {
                                ExecutorService executor = createExecutorForSpecificDataSource(slave);
                                getDataSourceSpecificExecutors().put(slave.getId(), executor);
                            }
                        }
                    }
                }
            }
        }
    }

    private ExecutorService createExecutorForSpecificDataSource(AtomDS ds) {
        final ExecutorService executor = createCustomExecutorService(ds.getCoreSize(), ds.getPoolSize(), "executor of data source:" + ds.getId());
        internalExecutorServiceRegistry.add(executor);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (executor == null) {
                    return;
                }
                try {
                    executor.shutdown();
                    executor.awaitTermination(5, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    logger.warn("interrupted when shuting down the query executor:\n{}", e);
                }
            }
        });
        return executor;
    }


    private ExecutorService createCustomExecutorService(int coreSize, int poolSize, final String name) {
        if (coreSize <= 0) {
            coreSize = Runtime.getRuntime().availableProcessors();
        }
        if (poolSize > 0 && poolSize < coreSize) {
            coreSize = poolSize;
        } else if (poolSize <= 0) {
            poolSize = coreSize * 2;
        }
        ThreadFactory tf = new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, name);
                t.setDaemon(true);
                return t;
            }
        };
        BlockingQueue<Runnable> queueToUse = new LinkedBlockingQueue<Runnable>();
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(coreSize, poolSize, 60, TimeUnit.SECONDS, queueToUse, tf, new ThreadPoolExecutor.CallerRunsPolicy());

        return executor;
    }

    public Map<String, ExecutorService> getDataSourceSpecificExecutors() {
        return dataSourceSpecificExecutors;
    }

    /**
     * Proxy needed to route MyBatis method calls to the proper SqlSession got
     * from Spring's Transaction Manager
     * It also unwraps exceptions thrown by {@code Method#invoke(Object, Object...)} to
     * pass a {@code PersistenceException} to the {@code PersistenceExceptionTranslator}.
     */
    private class DalSqlSessionInterceptor implements InvocationHandler {
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {


            SqlSession sqlSession = getSqlSession(
                    DalSqlSessionTemplate.this.sqlSessionFactory,
                    DalSqlSessionTemplate.this.executorType,
                    DalSqlSessionTemplate.this.exceptionTranslator);

            rewriteSqlMapClient(sqlSession, DalSqlSessionTemplate.this.sqlSessionFactory);

            try {
                Object result = method.invoke(sqlSession, args);
                if (!isSqlSessionTransactional(sqlSession, DalSqlSessionTemplate.this.sqlSessionFactory)) {
                    // force commit even on non-dirty sessions because some databases require
                    // a commit/rollback before calling close()
                    sqlSession.commit(true);
                }
                return result;
            } catch (Throwable t) {
                Throwable unwrapped = unwrapThrowable(t);
                if (DalSqlSessionTemplate.this.exceptionTranslator != null && unwrapped instanceof PersistenceException) {
                    // release the connection to avoid a deadlock if the translator is no loaded. See issue #22
                    closeSqlSession(sqlSession, DalSqlSessionTemplate.this.sqlSessionFactory);
                    sqlSession = null;
                    Throwable translated = DalSqlSessionTemplate.this.exceptionTranslator.translateExceptionIfPossible((PersistenceException) unwrapped);
                    if (translated != null) {
                        unwrapped = translated;
                    }
                }
                throw unwrapped;
            } finally {
                if (sqlSession != null) {
                    closeSqlSession(sqlSession, DalSqlSessionTemplate.this.sqlSessionFactory);
                }
            }
        }

        private void rewriteSqlMapClient(SqlSession sqlSession, SqlSessionFactory sqlSessionFactory) {
            if (sqlMapExtensible == null) {
                sqlMapExtensible = new SqlMapExtensibleImpl();
            }
            try {
                sqlMapExtensible.rewrite(sqlSession, sqlSessionFactory.getConfiguration());
            } catch (Exception e) {
                logger.info("Error with SqlMapExtensible", e);
            }
        }

    }

    public IConcurrentRequestProcessor getConcurrentRequestProcessor() {
        return concurrentRequestProcessor;
    }

    protected Map<AtomDS, List<ShardingMapping>> lookupDataSourcesByRouter(final SqlCommandType statementType, final String statementName,
                                                                           Object object, Collection<String> tableNames) {
        Boolean mdbFlag = null;
        String groupDSName = null;
        List<ShardingMapping> virtualTablesMapping = null;
        RouterFactCtxVO routerFact = RouterFactCtx.getRfvoholder();
        if (routerFact != null) {
            mdbFlag = routerFact.getMdbFlag();
            groupDSName = routerFact.getGroupDSName();
            virtualTablesMapping = routerFact.getVirtualTablesMapping();
        }

        if (mdbFlag == null || !mdbFlag) {
            if (!statementType.equals(SqlCommandType.SELECT) || TransactionUtil.isTransactional()) {
                mdbFlag = true;
            }
        }
        ShardingFactDO shardingFact = new ShardingFactDO();
        shardingFact.setIsMaster(mdbFlag);
        shardingFact.setTables(tableNames);
        shardingFact.setStatementName(statementName);
        shardingFact.setParameterObject(object);
        if (groupDSName != null) {
            shardingFact.setGroupDS(groupDSName);
        }

        if (!CollectionUtils.isEmpty(tableNames) && CollectionUtils.isEmpty(virtualTablesMapping)) {
            virtualTablesMapping = dalclientService.getVirtualTablesMapping(shardingFact);
        }
        if (!CollectionUtils.isEmpty(virtualTablesMapping)) {
            RouterFactCtxVO fact = RouterFactHelper.getCurrentFact();
            fact.setVirtualTablesMapping(virtualTablesMapping);
            shardingFact.setVirtualTablesMapping(virtualTablesMapping);
        }

        Map<AtomDS, List<ShardingMapping>> results = dalclientService.getAtomDataSources(shardingFact);
        return results;
    }

    public void setDalclientService(RoutingFacade dalclientService) {
        this.dalclientService = dalclientService;
    }

    protected boolean isPartitioningBehaviorEnabled() {
        return (dalclientService != null);
    }

    public boolean isExecuteQueryInConcurrency(Object parameterObject) {
        RouterFactCtxVO fact = RouterFactCtx.getRfvoholder();
        if (fact != null) {
            Boolean isExecute = fact.getExecuteQueryInConcurrency();
            if (isExecute != null) {
                return isExecute;
            }
        }
        if (executeQueryInConcurrency) {
            return true;
        }
        return false;
    }

    private void setShardingParameters(Object parameterObject, ShardingMapping mapping) {
        Object factParameters = mapping.getParameterObjects();
        if (parameterObject instanceof Map) {
            Map parameters = (Map) parameterObject;
            Object list = ((Map) parameterObject).get("list");
            if (list instanceof Collection || list != null && list.getClass().isArray()) {
                Map newParameters = new HashMap(parameters);
                newParameters.put("list", mapping.getParameterObjects());
                factParameters = newParameters;
            } else {
                factParameters = parameters;
            }
        }
        RouterFactHelper.getCurrentFact().setParameters(factParameters);
        List<ShardingMapping> virtualTablesMapping = new ArrayList<>();
        virtualTablesMapping.add(mapping);
        RouterFactHelper.getCurrentFact().setVirtualTablesMapping(virtualTablesMapping);
    }

    private boolean isShardingOperation(SqlCommandType statementType, Map<AtomDS, List<ShardingMapping>> dataSources) {
        if (dataSources.size() > 1) {
            return true;
        }
        List<ShardingMapping> virtualTablesMapping = dataSources.entrySet().iterator().next().getValue();
        if (virtualTablesMapping != null) {
            boolean hasShardingTab = virtualTablesMapping.size() > 1;
            if (hasShardingTab) {
                if (statementType.equals(SqlCommandType.SELECT) && this.isMergeShardingQueriesInSameDataSource()) {
                    return false;
                }
                return true;
            }
        }

        return false;
    }

    public boolean isMergeShardingQueriesInSameDataSource() {
        return mergeShardingQueriesInSameDataSource;
    }

    public boolean isExecuteDeleteInConcurrency(Object parameterObject) {
        RouterFactCtxVO fact = RouterFactCtx.getRfvoholder();
        if (fact != null) {
            Boolean isExecute = fact.getExecuteDeleteInConcurrency();
            if (isExecute != null) {
                return isExecute;
            }
        }
        if (executeDeleteInConcurrency) {
            return true;
        }
        return false;
    }
}
