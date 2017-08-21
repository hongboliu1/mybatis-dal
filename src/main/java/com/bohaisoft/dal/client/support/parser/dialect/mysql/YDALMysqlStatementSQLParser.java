package com.bohaisoft.dal.client.support.parser.dialect.mysql;

import java.util.List;
import java.util.Set;

import com.bohaisoft.dal.client.support.parser.SQLParserHelper;
import org.springframework.util.CollectionUtils;

import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

public class YDALMysqlStatementSQLParser {

	public static void parseSQL(Set<String> tableSet, String sql, SqlType type) {
		MySqlStatementParser sp = new MySqlStatementParser(sql);
		switch (type) {
		case INSERT:
//			tableSet.add(((MySqlInsertStatement) sp.parseInsert()).getTableName().toString());
			MySqlInsertStatement insertStatement = (MySqlInsertStatement) sp.parseInsert();
			tableSet.add(insertStatement.getTableName().toString());
			SQLParserHelper.parseSubTablesOfInsertSelectStatement(tableSet, insertStatement);
			
			return;
		case REPLACE:
			tableSet.add(sp.parseReplicate().getTableName().toString());
			return;
		case UPDATE:
			SQLUpdateStatement updateStatement = (SQLUpdateStatement) sp.parseUpdateStatement();
			tableSet.add(updateStatement.getTableName().toString());
			SQLParserHelper.parseSubTablesOfUpdateStatement(tableSet, updateStatement);
			return;
		case DELETE:
			SQLDeleteStatement deleteStatement = (SQLDeleteStatement) sp.parseDeleteStatement();
			tableSet.add(deleteStatement.getTableName().toString());
			SQLParserHelper.parseSubTablesOfDeleteStatement(tableSet, deleteStatement);
			return;
		case TRUNCATE: {
			List<SQLExprTableSource> tables = ((SQLTruncateStatement) sp.parseTruncate()).getTableSources();
			if (!CollectionUtils.isEmpty(tables)) {
				tableSet.add(tables.get(0).toString());
			}
			return;
		}
		default:
			return;
		}
	}

	public static enum SqlType {
		INSERT, UPDATE, DELETE, REPLACE, TRUNCATE;

	}
}
