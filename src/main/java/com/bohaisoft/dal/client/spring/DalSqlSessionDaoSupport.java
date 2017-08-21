package com.bohaisoft.dal.client.spring;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.dao.support.DaoSupport;

import static org.springframework.util.Assert.notNull;

/**
 * Created by liuhb on 2017/1/5.
 */
public class DalSqlSessionDaoSupport extends DaoSupport {

    private SqlSession sqlSession;

    private boolean externalSqlSession;

    public void setSqlSessionFactory(SqlSessionFactory sqlSessionFactory) {
        if (!this.externalSqlSession) {
            this.sqlSession = new DalSqlSessionTemplate(sqlSessionFactory);
        }
    }

    public void setDalSqlSessionTemplate(DalSqlSessionTemplate dalSqlSessionTemplate) {
        this.sqlSession = dalSqlSessionTemplate;
        this.externalSqlSession = true;
    }

    /**
     * Users should use this method to get a SqlSession to call its statement methods
     * This is SqlSession is managed by spring. Users should not commit/rollback/close it
     * because it will be automatically done.
     *
     * @return Spring managed thread safe SqlSession
     */
    public SqlSession getSqlSession() {
        return this.sqlSession;
    }

    /**
     * {@inheritDoc}
     */
    protected void checkDaoConfig() {
        notNull(this.sqlSession, "Property 'sqlSessionFactory' or 'sqlSessionTemplate' are required");
    }
}
