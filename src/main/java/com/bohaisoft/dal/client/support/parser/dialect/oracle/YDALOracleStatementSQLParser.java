package com.bohaisoft.dal.client.support.parser.dialect.oracle;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLMergeStatement;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleDeleteStatement;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleInsertStatement;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleUpdateStatement;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.alibaba.druid.sql.parser.ParserException;
import com.bohaisoft.dal.client.support.parser.SQLParserHelper;

import java.util.Set;

public class YDALOracleStatementSQLParser {

    public static void parseSQL(Set<String> tableSet, String sql, SqlType type) {
        OracleStatementParser sp = new OracleStatementParser(sql);
        switch (type) {
            case INSERT:
                OracleInsertStatement insertStatement = (OracleInsertStatement) sp.parseInsert();
                tableSet.add(insertStatement.getTableName().toString());
                SQLParserHelper.parseSubTablesOfInsertSelectStatement(tableSet, insertStatement);
                return;
            case UPDATE:
                OracleUpdateStatement updateStatement = (OracleUpdateStatement) sp.parseUpdateStatement();
                tableSet.add(updateStatement.getTableName().toString());
                SQLParserHelper.parseSubTablesOfUpdateStatement(tableSet, updateStatement);
                return;
            case DELETE:
                OracleDeleteStatement deleteStatement = (OracleDeleteStatement) sp.parseDeleteStatement();
                tableSet.add(deleteStatement.getTableName().toString());
                SQLParserHelper.parseSubTablesOfDeleteStatement(tableSet, deleteStatement);
                return;
            case TRUNCATE:
                tableSet.add(((SQLIdentifierExpr) (((SQLTruncateStatement) sp.parseTruncate()).getTableSources().get(0).getExpr())).getName());
                return;
            case MERGE:
                tableSet.add(((SQLMergeStatement) sp.parseMerge()).getInto().toString());
                return;
            default:
                throw new ParserException();
        }
    }

    public static enum SqlType {
        INSERT, UPDATE, DELETE, TRUNCATE, MERGE;

    }
}
