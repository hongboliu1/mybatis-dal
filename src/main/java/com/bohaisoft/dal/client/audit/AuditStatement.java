package com.bohaisoft.dal.client.audit;

/**
 * audit statement for each sql execution
 * 
 * @author wuxiang
 * @since 2012-7-2
 */
public class AuditStatement {

	private String opType;
	private String statementName;
	private String errorMsg;
	private String dsName;
	private long interval;
	private String sql;
	private String transactionLevel;
	private String parameters;
	private String tables;

	public AuditStatement(String opType, String statementName, String errorMsg, String dsName, long interval, String sql, String parameters, String transactionLevel ,String tables) {
		this.opType = opType;
		this.statementName = statementName;
		this.errorMsg = errorMsg;
		this.dsName = dsName;
		this.interval = interval;
		this.sql = sql;
		this.parameters = parameters;
		this.transactionLevel = transactionLevel;
		this.tables = tables;
	}

	public String getTransactionLevel() {
		return transactionLevel;
	}

	public void setTransactionLevel(String transactionLevel) {
		this.transactionLevel = transactionLevel;
	}

	public String getParameters() {
		return parameters;
	}

	public void setParameters(String parameters) {
		this.parameters = parameters;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getOpType() {
		return opType;
	}

	public void setOpType(String opType) {
		this.opType = opType;
	}

	public String getStatementName() {
		return statementName;
	}

	public void setStatementName(String statementName) {
		this.statementName = statementName;
	}

	public String getErrorMsg() {
		return errorMsg;
	}

	public void setErrorMsg(String errorMsg) {
		this.errorMsg = errorMsg;
	}

	public String getDsName() {
		return dsName;
	}

	public void setDsName(String dsName) {
		this.dsName = dsName;
	}

	public long getInterval() {
		return interval;
	}

	public void setInterval(long interval) {
		this.interval = interval;
	}

	public String getTables() {
		return tables;
	}

	public void setTables(String tables) {
		this.tables = tables;
	}

	
}
