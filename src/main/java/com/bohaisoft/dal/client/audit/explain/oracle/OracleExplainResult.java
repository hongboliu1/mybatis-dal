/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */
package com.bohaisoft.dal.client.audit.explain.oracle;


import com.bohaisoft.dal.client.audit.explain.IExplainResult;

/**
 * 
 * 
 * @author wuxiang
 * @since 2012-8-10
 */
public class OracleExplainResult implements IExplainResult {

	private String type;
	private String extra;
	private Integer cost;
	private Integer rows;
	private boolean badSql;
	private String detail;

	public Integer getRows() {
		return rows;
	}

	public void setRows(Integer rows) {
		this.rows = rows;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getExtra() {
		return extra;
	}

	public void setExtra(String extra) {
		this.extra = extra;
	}

	public Integer getCost() {
		return cost;
	}

	public void setCost(Integer cost) {
		this.cost = cost;
	}

	public boolean isBadSql() {
		return badSql;
	}

	public void setBadSql(boolean badSql) {
		this.badSql = badSql;
	}

	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append("type-").append(type).append(",cost-").append(cost).append(",extra-").append(extra);

		return str.toString();
	}

	public void setDetail(String detail) {
		this.detail = detail;
	}

	@Override
	public String getDetail() {
		return detail;
	}
}
