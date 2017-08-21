package com.bohaisoft.dal.client.audit;

import java.util.Map;

/**
 * audit context
 * 
 * @author wuxiang
 * @since 2012-7-3
 */
public class AuditContext {

	private long auditPeriod;
	private int queueSize;
	private Map<String, Map<String, Integer>> statementCountResults = null;
	private Map<String, AuditStatement> sqlStatementResults = null;

	public int getQueueSize() {
		return queueSize;
	}

	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	public long getAuditPeriod() {
		return auditPeriod;
	}

	public void setAuditPeriod(long auditPeriod) {
		this.auditPeriod = auditPeriod;
	}

	public Map<String, Map<String, Integer>> getStatementCountResults() {
		return statementCountResults;
	}

	public void setStatementCountResults(Map<String, Map<String, Integer>> statementCountResults) {
		this.statementCountResults = statementCountResults;
	}

	public Map<String, AuditStatement> getSqlStatementResults() {
		return sqlStatementResults;
	}

	public void setSqlStatementResults(Map<String, AuditStatement> sqlStatementResults) {
		this.sqlStatementResults = sqlStatementResults;
	}

}
