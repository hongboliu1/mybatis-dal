/**
 * Copyright(c) 2012 yihaodian. All rights reserved.
 * dal-client-1.1.0
 */
package com.bohaisoft.dal.client.concurrent;

import org.apache.ibatis.session.SqlSession;

import java.util.List;

/**
 *
 *
 * @author wuxiang
 * @since 2013-1-7
 */
public interface IConcurrentRequestProcessor {

    <E> List<E> process(SqlSession sqlSession, List<ConcurrentRequest> requests, boolean earlyReleaseConnection);

    void setConcurrentExecuteTimeout(long concurrentExecuteTimeout);
}
