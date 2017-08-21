/**
 * Copyright(c) 2012 yihaodian. All rights reserved.
 * dal-client
 */

package com.bohaisoft.dal.client.router.support.mybatis.version3_2_7;


import com.bohaisoft.dal.client.router.sql.SqlRewriter;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.ibatis.executor.BaseExecutor;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

/**
 * 扩展ibatis自带的SqlExecutor(ibatis version:2.3.0)
 * 通过扩展的SqlExecutor来替换SQL中的虚拟表名为实际分表的表名称
 *
 * @author wuxiang
 * @since 2012-3-20
 */
public class RouterSqlExecutor extends BaseExecutor {

    static final Logger logger = LoggerFactory.getLogger(RouterSqlExecutor.class);
    private SqlRewriter sqlRewriter = new SqlRewriter();

    public RouterSqlExecutor(Configuration configuration, Transaction transaction) {
        super(configuration, transaction);
    }


    @Override
    protected int doUpdate(MappedStatement ms, Object parameter) throws SQLException {
        Statement stmt = null;
        try {
            Configuration configuration = ms.getConfiguration();
            BoundSql boundSql = ms.getBoundSql(parameter);
            String newSql = sqlRewriter.replaceVirtualTable(boundSql.getSql(), null);
            FieldUtils.writeField(boundSql, "sql", newSql, true);
            StatementHandler handler = configuration.newStatementHandler(this, ms, parameter, RowBounds.DEFAULT, null, boundSql);
            stmt = prepareStatement(handler, ms.getStatementLog());
            return handler.update(stmt);
        }catch (IllegalAccessException e) {
            logger.error(e.getMessage(),e);
            throw new SQLException(e.getMessage());
        } finally {
            closeStatement(stmt);
        }
    }

    @Override
    protected List<BatchResult> doFlushStatements(boolean isRollback) throws SQLException {
        return Collections.emptyList();
    }

    @Override
    protected <E> List<E> doQuery(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) throws SQLException {
        Statement stmt = null;
        try {
            Configuration configuration = ms.getConfiguration();
            String newSql = sqlRewriter.replaceVirtualTable(boundSql.getSql(), null);
            FieldUtils.writeField(boundSql, "sql", newSql, true);
            StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, resultHandler, boundSql);
            stmt = prepareStatement(handler, ms.getStatementLog());
            return handler.<E>query(stmt, resultHandler);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage(),e);
            throw new SQLException(e.getMessage());
        } finally {
            closeStatement(stmt);
        }
    }

    private Statement prepareStatement(StatementHandler handler, Log statementLog) throws SQLException {
        Statement stmt;
        Connection connection = getConnection(statementLog);
        stmt = handler.prepare(connection);
        handler.parameterize(stmt);
        return stmt;
    }
}
