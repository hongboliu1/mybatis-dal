package com.bohaisoft.dal.client.concurrent;

import org.apache.ibatis.session.SqlSession;

import java.sql.SQLException;

/**
 * Created by liuhb on 2017/1/22.
 */
public interface SqlMapClientCallback {

    Object doInSqlMapClient(SqlSession sqlSessionProxy) throws SQLException;
}
