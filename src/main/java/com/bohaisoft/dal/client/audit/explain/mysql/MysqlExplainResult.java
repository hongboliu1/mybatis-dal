/**
 * Copyright(c) 2012 yihaodian. All rights reserved.
 * dal-client
 */
package com.bohaisoft.dal.client.audit.explain.mysql;

import com.bohaisoft.dal.client.audit.explain.IExplainResult;

/**
 * @author wuxiang
 * @since 2012-8-10
 */
public class MysqlExplainResult implements IExplainResult {

    private String type;
    private String extra;
    private Integer rows;
    private boolean badSql;

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

    public Integer getRows() {
        return rows;
    }

    public void setRows(Integer rows) {
        this.rows = rows;
    }

    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("type-").append(type).append(",rows-").append(rows).append(",extra-").append(extra);

        return str.toString();
    }

    @Override
    public String getDetail() {
        return toString();
    }

    @Override
    public void setBadSql(boolean badSql) {
        this.badSql = badSql;
    }

    @Override
    public boolean isBadSql() {
        return badSql;
    }
}
