package com.bohaisoft.dal.client.transaction;

import com.bohaisoft.dal.client.transaction.support.TransactionContext;
import com.bohaisoft.dal.client.transaction.support.TransactionObject;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;


/**
 * 
 * 跨库事务管理器，封装多个数据源的事务管理器
 * @author wuxiang
 * @since 2012-7-5
 */
public class MultipleDsOndemandTransactionManager extends AbstractPlatformTransactionManager {

	private static final long serialVersionUID = 4712923770419532385L;

	public MultipleDsOndemandTransactionManager() {
		this.setTransactionSynchronization(getTransactionSynchronization());
		setNestedTransactionAllowed(true);
	}

	@Override
	protected Object doGetTransaction() throws TransactionException {
		TransactionObject txObject = TransactionContext.getTransactionObject();
		if (txObject == null) {
			txObject = new TransactionObject();
			TransactionContext.setTransactionObject(txObject);
		}
		return txObject;
	}

	@Override
	protected boolean isExistingTransaction(Object transaction) throws TransactionException {
		TransactionObject txObject = (TransactionObject) transaction;
		return txObject.isExistingTransaction();
	}

	@Override
	protected void doCleanupAfterCompletion(Object transaction) {
		TransactionContext.clear();
	}
	
	@Override
	protected void doSetRollbackOnly(DefaultTransactionStatus status) throws TransactionException {
		TransactionObject txObject = (TransactionObject) status.getTransaction();
		txObject.setRollbackOnly();
	}

	protected Object doSuspend(Object transaction) {
		TransactionObject txObject = (TransactionObject) transaction;
		return txObject.doSuspend(transaction);
	}

	protected void doResume(Object transaction, Object suspendedResources) {
		TransactionObject txObject = (TransactionObject) transaction;
		txObject.doResume(transaction, suspendedResources);
	}

	@Override
	protected void doBegin(Object transaction, TransactionDefinition transactionDefinition) throws TransactionException {
		TransactionObject txObject = (TransactionObject) transaction;
		txObject.doBegin(transactionDefinition);
	}

	@Override
	protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
		TransactionObject txObject = (TransactionObject) status.getTransaction();
		txObject.doCommit(status);
	}

	@Override
	protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
		TransactionObject txObject = (TransactionObject) status.getTransaction();
		txObject.doRollback(status);
	}
}
