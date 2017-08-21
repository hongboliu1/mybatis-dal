/**
 * Copyright(c) 2012 yihaodian. All rights reserved.
 * dal-client-1.1.0
 */
package com.bohaisoft.dal.client.concurrent;

import org.apache.ibatis.session.SqlSessionFactory;

import java.sql.Connection;

/**
 * @author wuxiang
 * @since 2013-1-7
 */
public class ConcurrentRequestWrapper {

    private ConcurrentRequestHolder requestHolder;
    private ConcurrentRequest originalRequest;
    private boolean transactionAware;
    private Connection connection;

    public ConcurrentRequestHolder getRequestHolder() {
        return requestHolder;
    }

    public void setRequestHolder(ConcurrentRequestHolder requestHolder) {
        this.requestHolder = requestHolder;
    }

    public ConcurrentRequest getOriginalRequest() {
        return originalRequest;
    }

    public void setOriginalRequest(ConcurrentRequest originalRequest) {
        this.originalRequest = originalRequest;
    }

    public boolean isTransactionAware() {
        return transactionAware;
    }

    public void setTransactionAware(boolean transactionAware) {
        this.transactionAware = transactionAware;
    }

    public void releaseConnection() {
        if (requestHolder != null) {
            requestHolder.releaseRequest(this);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
