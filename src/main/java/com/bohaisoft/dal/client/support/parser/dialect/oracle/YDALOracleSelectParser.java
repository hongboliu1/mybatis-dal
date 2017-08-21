package com.bohaisoft.dal.client.support.parser.dialect.oracle;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleSelectParser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class YDALOracleSelectParser {

    public static HashSet<String> parseQuerySQL(String sql) {
        OracleSelectParser sp = new OracleSelectParser(sql);
        SQLSelect ss = sp.select();
        List<SQLSelectQueryBlock> queries = new ArrayList<SQLSelectQueryBlock>();
        HashSet<String> tables = new HashSet<String>();
        getSQLSelectQueryBlock(ss.getQuery(), queries, tables);
        return tables;
    }

    private static void getSQLSelectQueryBlock(SQLSelectQuery sq, List<SQLSelectQueryBlock> queries, HashSet<String> tables) {
        if (sq instanceof SQLSelectQueryBlock) {
            SQLTableSource source = ((SQLSelectQueryBlock) sq).getFrom();
            populateTableSource(source, queries, tables);
            queries.add((SQLSelectQueryBlock) sq);
        } else if (sq instanceof SQLUnionQuery) {
            SQLUnionQuery squ = (SQLUnionQuery) sq;
            SQLSelectQuery lq = squ.getLeft();
            getSQLSelectQueryBlock(lq, queries, tables);
            SQLSelectQuery rq = squ.getRight();
            getSQLSelectQueryBlock(rq, queries, tables);
        }
    }

    public static void populateTableSource(SQLTableSource source, List<SQLSelectQueryBlock> queries, HashSet<String> tables) {
        if (source instanceof SQLJoinTableSource) {
            SQLTableSource ls = ((SQLJoinTableSource) source).getLeft();
            populateTableSource(ls, queries, tables);
            SQLTableSource rs = ((SQLJoinTableSource) source).getRight();
            populateTableSource(rs, queries, tables);
        } else if (source instanceof SQLExprTableSource) {
            SQLExpr er = ((SQLExprTableSource) source).getExpr();
            if (er instanceof SQLIdentifierExpr) {
                tables.add(((SQLIdentifierExpr) er).getName());
            } else if (er instanceof SQLPropertyExpr) {
                tables.add(((SQLPropertyExpr) er).getName());
            }
        } else if (source instanceof SQLSubqueryTableSource) {
            SQLSelectQuery subq = ((SQLSubqueryTableSource) source).getSelect().getQuery();
            getSQLSelectQueryBlock(subq, queries, tables);

        }
    }

}
