package com.bohaisoft.dal.client.router.support.mybatis;


import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;

public interface SqlMapExtensible {

	/**
	 * 扩展方法
	 * @param
	 * @throws Exception
	 */
	void rewrite(SqlSession sqlSession, Configuration configuration) throws Exception;
	
}
