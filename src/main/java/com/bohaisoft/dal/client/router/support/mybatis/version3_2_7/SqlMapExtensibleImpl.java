package com.bohaisoft.dal.client.router.support.mybatis.version3_2_7;


import com.bohaisoft.dal.client.router.support.mybatis.SqlMapExtensible;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.ibatis.executor.CachingExecutor;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.defaults.DefaultSqlSession;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.managed.ManagedTransactionFactory;

/**
 * 通过反射扩展ibatis的sqlExecutor对象
 *
 * @author wuxiang
 * @since 2013-1-7
 */
public class SqlMapExtensibleImpl implements SqlMapExtensible {

    @Override
    public void rewrite(SqlSession sqlSession, Configuration configuration) throws Exception {
        DefaultSqlSession defaultSqlSession = (DefaultSqlSession) sqlSession;
        final Environment environment = configuration.getEnvironment();
        final TransactionFactory transactionFactory = getTransactionFactoryFromEnvironment(environment);
        Transaction tx = transactionFactory.newTransaction(environment.getDataSource(), null, false);
        Executor executor = new RouterSqlExecutor(configuration, tx);
        executor = new CachingExecutor(executor);
        FieldUtils.writeField(defaultSqlSession, "executor", executor, true);
    }

    private TransactionFactory getTransactionFactoryFromEnvironment(Environment environment) {
        if (environment == null || environment.getTransactionFactory() == null) {
            return new ManagedTransactionFactory();
        }
        return environment.getTransactionFactory();
    }

}
