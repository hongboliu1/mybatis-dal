package com.bohaisoft.dal.client.transaction.support;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.bohaisoft.dal.client.datasource.AtomDS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.support.DefaultTransactionStatus;


/**
 * Created by IntelliJ IDEA. User: zhanghonglun Date: 11-12-6 Time: 上午10:11 To
 * change this template use File | Settings | File Templates.
 */
public class TransactionUtil {
	private static Map<String, DataSourceTransactionManager> txContainer = new HashMap<>();
	static final Logger logger = LoggerFactory.getLogger(TransactionUtil.class);

	public static boolean isTransactional() {
		TransactionObject txObj = TransactionContext.getTransactionObject();
		if(txObj != null && txObj.isExistingTransaction()) {
			return true;
		}
		return false;
	}
	
	public static void beginAtomDSTransaction(AtomDS atomDS) {
		String dsId = atomDS.getTargetId();
		TransactionObject txObj = TransactionContext.getTransactionObject();
		if (txObj != null && txObj.isExistingTransaction() && !txObj.isExistingDataSource(dsId) && atomDS.isNeedTransaction()) {
			DataSourceTransactionManager txManager = txContainer.get(dsId);
			if (txManager == null) {
				txManager = new DataSourceTransactionManager(atomDS.getTargetDataSource());
				txContainer.put(dsId, txManager);
			}
			DefaultTransactionStatus transactionStatus;
			try {
				transactionStatus = (DefaultTransactionStatus) txManager.getTransaction(txObj.currentTransactionDefinition());
			} catch (CannotCreateTransactionException e) {
				if(atomDS.getParentDS() != null && atomDS.getParentDS().isPassiveFailoverEnable()) {
					try {
						txManager = new DataSourceTransactionManager(atomDS.getTargetDataSource());
						transactionStatus = (DefaultTransactionStatus) txManager.getTransaction(txObj.currentTransactionDefinition());
					} catch (Exception ex) {
						logger.error(ex.getMessage(), e);
						throw e;
					}
				} else {
					throw e;
				}
			}
			txObj.addTransactionStatus(transactionStatus, dsId, atomDS.getTargetDataSource(), txManager);
		}
	}
}
