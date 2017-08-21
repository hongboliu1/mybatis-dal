/**
 * Copyright(c) 2012 yihaodian. All rights reserved.
 * dal-client
 */
package com.bohaisoft.dal.client.transaction.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.JdbcTransactionObjectSupport;
import org.springframework.transaction.SavepointManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;

/**
 * 事务对象，用于事务的提交或回滚，实现了1阶段事务提交，支持REQUIRED,REQUIRES_NEW等事务
 * 暂未支持NESTED事务，以后可以支持2阶段事务提交
 *
 * @author wuxiang
 * @since 2012-7-5
 */
public class TransactionObject implements SavepointManager, SmartTransactionObject {

    static final Logger logger = LoggerFactory.getLogger(TransactionObject.class);

    boolean rollbackOnly = false;
    TreeMap<String, Map<JdbcTransactionObjectSupport, DefaultTransactionStatus>> txSavepointMap = new TreeMap<String, Map<JdbcTransactionObjectSupport, DefaultTransactionStatus>>();

    List<TransactionStatus> transactionStatusList = new ArrayList<>();

    private class TransactionStatus {
        String id;
        List<TransactionHolder> transactionHolders;
        TransactionDefinition transactionDefinition;
        Map<DataSource, ConnectionHolder> dataSourceHolders;

        public void setDataSourceHolders(Map<DataSource, ConnectionHolder> dataSourceHolders) {
            this.dataSourceHolders = dataSourceHolders;
        }

        public Map<DataSource, ConnectionHolder> getDataSourceHolders() {
            return dataSourceHolders;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public List<TransactionHolder> getTransactionHolders() {
            return transactionHolders;
        }

        public void setTransactionHolders(List<TransactionHolder> transactionHolders) {
            this.transactionHolders = transactionHolders;
        }

        public TransactionDefinition getTransactionDefinition() {
            return transactionDefinition;
        }

        public void setTransactionDefinition(TransactionDefinition transactionDefinition) {
            this.transactionDefinition = transactionDefinition;
        }

    }

    public void doCommit(DefaultTransactionStatus status) {
        TransactionStatus ts = currentTransactionStatus();
        List<TransactionHolder> transactionHolderList;
        Map<DataSource, ConnectionHolder> dataSourceConnectionHolderMap;
        if (ts != null) {
            transactionHolderList = ts.getTransactionHolders();
            dataSourceConnectionHolderMap = ts.getDataSourceHolders();
        } else {
            throw new IllegalStateException("No transaction found");
        }
        String id = ts.getId();
        if (logger.isDebugEnabled()) {
            logger.debug("transaction: " + id + " committed");
        }

        TransactionException lastException = null;
        if (transactionHolderList != null) {
            for (TransactionHolder th : transactionHolderList) {
                DataSourceTransactionManager dataSourceTransactionManager = th.getDataSourceTransactionManager();
                ConnectionHolder connectionHolder = dataSourceConnectionHolderMap.get(dataSourceTransactionManager.getDataSource());
                try {
                    if (lastException == null) {
                        connectionHolder.getConnection().commit();
                        //dataSourceTransactionManager.commit(th.getDefaultTransactionStatus());
                    } else { // 如果有事务提交失败，所有后面的事务回滚，已经提交的部分有必要的话需要业务补偿
                        logger.error("Datasource " + th.getDsId() + " " + id + "commit failed");
                        connectionHolder.getConnection().rollback();
                        //dataSourceTransactionManager.rollback(th.getDefaultTransactionStatus());
                    }
                } catch (TransactionException e) {
                    logger.error("Datasource " + th.getDsId() + " " + id + "commit failed", e);
                    if (lastException == null) {
                        lastException = e;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        removeTransaction(ts);
        if (lastException != null) {
            throw lastException;
        }
    }

    public void doRollback(DefaultTransactionStatus status) {
        TransactionStatus ts = currentTransactionStatus();
        List<TransactionHolder> transactionHolderList;
        Map<DataSource, ConnectionHolder> dataSourceConnectionHolderMap;
        if (ts != null) {
            transactionHolderList = ts.getTransactionHolders();
            dataSourceConnectionHolderMap = ts.getDataSourceHolders();
        } else {
            throw new IllegalStateException("No transaction found");
        }
        String id = ts.getId();
        if (logger.isDebugEnabled()) {
            logger.debug("transaction: " + id + " rollbacked");
        }
        TransactionException lastException = null;
        logger.info("prepare to rollback transactions on multiple data sources.");
        if (transactionHolderList != null) {
            for (TransactionHolder th : transactionHolderList) {
                DataSourceTransactionManager dataSourceTransactionManager = th.getDataSourceTransactionManager();
                ConnectionHolder connectionHolder = dataSourceConnectionHolderMap.get(dataSourceTransactionManager.getDataSource());

                try {
                    connectionHolder.getConnection().rollback();
                    //th.getDataSourceTransactionManager().rollback(th.getDefaultTransactionStatus());
                } catch (TransactionException e) {
                    logger.error("Datasource " + th.getDsId() + " " + id + "rollback failed");
                    lastException = e;
                }
                catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        removeTransaction(ts);
        if (lastException != null) {
            throw lastException;
        }
    }

    public String doBegin(TransactionDefinition transactionDefinition) {
        String id = UUID.randomUUID().toString();
        TransactionStatus status = new TransactionStatus();
        status.setId(id);
        status.setTransactionDefinition(transactionDefinition);
        transactionStatusList.add(status);
        if (logger.isDebugEnabled()) {
            logger.debug("transaction: " + id + " started");
        }
        return id;
    }

    public boolean isExistingTransaction() {
        return transactionStatusList.size() > 0;
    }

    private TransactionStatus currentTransactionStatus() {
        int size = transactionStatusList.size();
        if (size > 0) {
            return transactionStatusList.get(size - 1);
        } else {
            return null;
        }
    }

    public TransactionDefinition currentTransactionDefinition() {
        TransactionStatus ts = currentTransactionStatus();
        if (ts == null) {
            throw new IllegalStateException("No transaction found");
        }
        return ts.getTransactionDefinition();
    }

    public void removeTransaction(TransactionStatus ts) {
        transactionStatusList.remove(ts);
    }

    public boolean isExistingDataSource(String dsId) {
        boolean isExist = false;
        TransactionStatus status = currentTransactionStatus();
        if (status != null) {
            List<TransactionHolder> transactionHolders = status.getTransactionHolders();
            if (transactionHolders != null) {
                for (TransactionHolder th : transactionHolders) {
                    if (th.getDsId().equals(dsId)) {
                        isExist = true;
                        break;
                    }
                }
            }
        }
        return isExist;
    }

    public void addTransactionStatus(DefaultTransactionStatus transactionStatus, String dsId, DataSource ds, DataSourceTransactionManager txManager) {
        TransactionStatus status = currentTransactionStatus();
        if (status == null) {
            throw new IllegalStateException("No transaction found");
        }
        List<TransactionHolder> transactionHolders = status.getTransactionHolders();
        if (transactionHolders == null) {
            transactionHolders = new ArrayList<>();
            status.setTransactionHolders(transactionHolders);
        }
        TransactionHolder th = new TransactionHolder();
        th.setDataSourceTransactionManager(txManager);
        th.setDefaultTransactionStatus(transactionStatus);
        th.setDsId(dsId);
        status.getTransactionHolders().add(0, th);
        if (status.getDataSourceHolders() == null) {
            status.setDataSourceHolders(new HashMap<>());
        }
        ConnectionHolder conHolder =
                (ConnectionHolder) TransactionSynchronizationManager.getResource(ds);
        status.getDataSourceHolders().put(ds, conHolder);

        // Map<JdbcTransactionObjectSupport, DefaultTransactionStatus>
        // txSavePoints = txSavepointMap.get(txSavepointMap.lastKey());
        /*
         * if(txSavePoints != null) {
		 * transactionStatus.createAndHoldSavepoint();
		 * txSavePoints.put((JdbcTransactionObjectSupport) txObject,
		 * transactionStatus); }
		 */
    }

	/*
     * public boolean isSuspended() { return isSuspended; }
	 */

    public Object doSuspend(Object transaction) {
        TransactionStatus status = currentTransactionStatus();
        if (status == null) {
            throw new IllegalStateException("No transaction found");
        }
        Map<DataSource, ConnectionHolder> dataSourceHolders = status.getDataSourceHolders();
        if (dataSourceHolders != null) {
            int size = dataSourceHolders.size();
            if (size > 0) {
                for (DataSource ds : dataSourceHolders.keySet()) {
                    ConnectionHolder conHolder = (ConnectionHolder) TransactionSynchronizationManager.unbindResource(ds);
                    dataSourceHolders.put(ds, conHolder);
                }
                return dataSourceHolders;
            }
        }
        return null;
    }

    public void doResume(Object transaction, Object resource) {
        Map<DataSource, ConnectionHolder> dataSourceHolders = (Map<DataSource, ConnectionHolder>) resource;
        if (dataSourceHolders != null) {
            for (DataSource ds : dataSourceHolders.keySet()) {
                ConnectionHolder conHolder = dataSourceHolders.get(ds);
                TransactionSynchronizationManager.bindResource(ds, conHolder);
            }
        }
    }

    public void setRollbackOnly() {
        rollbackOnly = true;
        logger.debug("Setting transaction rollback-only");
    }

    public boolean isRollbackOnly() {
        return rollbackOnly;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.springframework.transaction.SavepointManager#createSavepoint()
     */
    @Override
    public Object createSavepoint() throws TransactionException {
        String spId = UUID.randomUUID().toString();
        txSavepointMap.put(spId, new HashMap<JdbcTransactionObjectSupport, DefaultTransactionStatus>());

        return spId;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.springframework.transaction.SavepointManager#rollbackToSavepoint(
     * java.lang.Object)
     */
    @Override
    public void rollbackToSavepoint(Object savepoint) throws TransactionException {
        Map<JdbcTransactionObjectSupport, DefaultTransactionStatus> txSavepoints = txSavepointMap.get(savepoint);
        if (txSavepoints != null) {
            for (JdbcTransactionObjectSupport txObject : txSavepoints.keySet()) {
                DefaultTransactionStatus status = txSavepoints.get(txObject);
                if (status != null) {
                    status.rollbackToHeldSavepoint();
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.springframework.transaction.SavepointManager#releaseSavepoint(java
     * .lang.Object)
     */
    @Override
    public void releaseSavepoint(Object savepoint) throws TransactionException {
        Map<JdbcTransactionObjectSupport, DefaultTransactionStatus> txSavepoints = txSavepointMap.get(savepoint);
        if (txSavepoints != null) {
            for (JdbcTransactionObjectSupport txObject : txSavepoints.keySet()) {
                DefaultTransactionStatus status = txSavepoints.get(txObject);
                if (status != null) {
                    status.releaseHeldSavepoint();
                }
            }
        }
    }

    @Override
    public void flush() {

    }
}
