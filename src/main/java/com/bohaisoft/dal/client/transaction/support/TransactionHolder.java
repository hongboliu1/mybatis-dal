package com.bohaisoft.dal.client.transaction.support;

import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

/**
 * Created by IntelliJ IDEA. User: zhanghonglun Date: 11-12-6 Time: 上午9:46 To
 * change this template use File | Settings | File Templates.
 */
public class TransactionHolder {
	private String dsId;
	private DefaultTransactionStatus defaultTransactionStatus;
	private DataSourceTransactionManager dataSourceTransactionManager;

	public String getDsId() {
		return dsId;
	}

	public void setDsId(String dsId) {
		this.dsId = dsId;
	}

	public DefaultTransactionStatus getDefaultTransactionStatus() {
		return defaultTransactionStatus;
	}

	public void setDefaultTransactionStatus(
			DefaultTransactionStatus defaultTransactionStatus) {
		this.defaultTransactionStatus = defaultTransactionStatus;
	}

	public DataSourceTransactionManager getDataSourceTransactionManager() {
		return dataSourceTransactionManager;
	}

	public void setDataSourceTransactionManager(
			DataSourceTransactionManager dataSourceTransactionManager) {
		this.dataSourceTransactionManager = dataSourceTransactionManager;
	}

}
