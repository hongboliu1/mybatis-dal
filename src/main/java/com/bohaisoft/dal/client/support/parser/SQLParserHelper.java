/**
 * Copyright(c) 2012 yihaodian. All rights reserved.
 * dal-client-1.1.0
 */
package com.bohaisoft.dal.client.support.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.*;

import java.util.List;
import java.util.Set;

/**
 *
 *
 * @author wuxiang
 * @since 2013-3-1
 */
public class SQLParserHelper {

    public static void parseSubTablesOfUpdateStatement(Set<String> tableSet, SQLUpdateStatement updateStatement) {
        List<SQLUpdateSetItem> items = updateStatement.getItems();
        if (items != null) {
            for (SQLUpdateSetItem item : items) {
                SQLExpr expr = item.getValue();
                if (expr instanceof SQLQueryExpr) {
                    SQLQueryExpr queryExpr = (SQLQueryExpr) expr;
                    if (expr != null) {
                        SQLSelect select = (SQLSelect) queryExpr.getSubQuery();
                        if (select != null) {
                            SQLSelectQueryBlock query = (SQLSelectQueryBlock) select.getQuery();
                            if (query != null && query.getFrom() != null) {
                                SQLIdentifierExpr idExpr = (SQLIdentifierExpr) ((SQLExprTableSource) query.getFrom()).getExpr();
                                if (idExpr != null) {
                                    tableSet.add(idExpr.getName());
                                }
                            }
                        }
                    }
                }
            }
        }
        SQLExpr whereExpr = updateStatement.getWhere();
        if (whereExpr instanceof SQLInSubQueryExpr) {
            SQLInSubQueryExpr subWhereExpr = (SQLInSubQueryExpr) whereExpr;
            SQLSelect select = subWhereExpr.getSubQuery();
            if (select != null) {
                SQLSelectQueryBlock query = (SQLSelectQueryBlock) select.getQuery();
                if (query != null && query.getFrom() != null) {
                    SQLIdentifierExpr idExpr = (SQLIdentifierExpr) ((SQLExprTableSource) query.getFrom()).getExpr();
                    if (idExpr != null) {
                        tableSet.add(idExpr.getName());
                    }
                }
            }
        }
    }

    public static void parseSubTablesOfDeleteStatement(Set<String> tableSet, SQLDeleteStatement deleteStatement) {
        SQLExpr whereExpr = deleteStatement.getWhere();
        if (whereExpr instanceof SQLInSubQueryExpr) {
            SQLInSubQueryExpr subWhereExpr = (SQLInSubQueryExpr) whereExpr;
            SQLSelect select = subWhereExpr.getSubQuery();
            if (select != null) {
                SQLSelectQueryBlock query = (SQLSelectQueryBlock) select.getQuery();
                if (query != null && query.getFrom() != null) {
                    SQLIdentifierExpr idExpr = (SQLIdentifierExpr) ((SQLExprTableSource) query.getFrom()).getExpr();
                    if (idExpr != null) {
                        tableSet.add(idExpr.getName());
                    }
                }
            }
        }
    }

    public static void parseSubTablesOfInsertSelectStatement(Set<String> tableSet, SQLInsertStatement insertSelectStatement) {
        if (insertSelectStatement != null) {
            SQLSelect select = insertSelectStatement.getQuery();
            if (select != null) {
                SQLSelectQueryBlock query = (SQLSelectQueryBlock) select.getQuery();
                if (query != null && query.getFrom() != null) {
                    SQLIdentifierExpr idExpr = (SQLIdentifierExpr) ((SQLExprTableSource) query.getFrom()).getExpr();
                    if (idExpr != null) {
                        tableSet.add(idExpr.getName());
                    }
                }
            }
        }
    }
}
