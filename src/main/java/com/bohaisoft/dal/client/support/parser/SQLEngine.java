package com.bohaisoft.dal.client.support.parser;

import com.alibaba.druid.sql.parser.Token;
import com.bohaisoft.dal.client.cache.ICache;
import com.bohaisoft.dal.client.exception.RoutingException;
import com.bohaisoft.dal.client.support.parser.dialect.mysql.YDALMySqlSelectParser;
import com.bohaisoft.dal.client.support.parser.dialect.mysql.YDALMysqlStatementSQLParser;
import com.bohaisoft.dal.client.support.parser.dialect.oracle.YDALOracleSelectParser;
import com.bohaisoft.dal.client.support.parser.dialect.oracle.YDALOracleStatementSQLParser;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.defaults.DefaultSqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class SQLEngine {
    static final Logger logger = LoggerFactory.getLogger(SQLEngine.class);
    public final static String PROCEDUREEXPRESSION = "\\{\\s*CALL\\s*";

    public final static String PROCEDURE = "{CALL ";

    public final static String LEFTPARENTHESIS = "(";

    private static SQLEngine instance = null;

    private ICache sqlCache = null;

    public ICache getSqlCache() {
        return sqlCache;
    }

    public void setSqlCache(ICache sqlCache) {
        this.sqlCache = sqlCache;
    }

    public static synchronized SQLEngine getInstance() {
        if (instance == null) {
            try {
                instance = new SQLEngine();
            } catch (Exception e) {
                // logger.error(e);
            }
        }
        return instance;
    }

    private SQLEngine() {
    }

    /**
     * @param sql
     * @return
     * @throws Exception
     */
    public Set<String> getTableNameSet(String sql) throws Exception {
        HashSet<String> tableSet = new HashSet<>();
        if (sql != null && !"".equals(sql)) {
            String as[] = sql.trim().split("\\s");
            if (as[0].toUpperCase().equals(Token.SELECT.name)) {
                try {
                    tableSet = YDALOracleSelectParser.parseQuerySQL(sql);
                } catch (Exception e) {
                    tableSet = YDALMySqlSelectParser.parseQuerySQL(sql);
                }
            } else {
                if (as[0].toUpperCase().equals(Token.INSERT.name)) {
                    try {
                        YDALOracleStatementSQLParser.parseSQL(tableSet, sql, YDALOracleStatementSQLParser.SqlType.INSERT);
                    } catch (Exception e) {
                        YDALMysqlStatementSQLParser.parseSQL(tableSet, sql, YDALMysqlStatementSQLParser.SqlType.INSERT);
                    }
                } else if (as[0].toUpperCase().equals("REPLACE")) {
                    YDALMysqlStatementSQLParser.parseSQL(tableSet, sql, YDALMysqlStatementSQLParser.SqlType.REPLACE);
                } else if (as[0].toUpperCase().equals(Token.DELETE.name)) {
                    try {
                        YDALOracleStatementSQLParser.parseSQL(tableSet, sql, YDALOracleStatementSQLParser.SqlType.DELETE);
                    } catch (Exception e) {
                        YDALMysqlStatementSQLParser.parseSQL(tableSet, sql, YDALMysqlStatementSQLParser.SqlType.DELETE);
                    }
                } else if (as[0].toUpperCase().equals(Token.UPDATE.name)) {
                    try {
                        YDALOracleStatementSQLParser.parseSQL(tableSet, sql, YDALOracleStatementSQLParser.SqlType.UPDATE);
                    } catch (Exception e) {
                        YDALMysqlStatementSQLParser.parseSQL(tableSet, sql, YDALMysqlStatementSQLParser.SqlType.UPDATE);
                    }
                } else if (as[0].toUpperCase().equals(Token.MERGE.name)) {

                    YDALOracleStatementSQLParser.parseSQL(tableSet, sql, YDALOracleStatementSQLParser.SqlType.MERGE);

                } else if (as[0].toUpperCase().equals(Token.TRUNCATE.name)) {
                    try {
                        YDALOracleStatementSQLParser.parseSQL(tableSet, sql, YDALOracleStatementSQLParser.SqlType.TRUNCATE);
                    } catch (Exception e) {
                        YDALMysqlStatementSQLParser.parseSQL(tableSet, sql, YDALMysqlStatementSQLParser.SqlType.TRUNCATE);
                    }
                } else {
                    Pattern pattern_procedure = Pattern.compile(PROCEDUREEXPRESSION);
                    Matcher re_procedure = pattern_procedure.matcher(sql);
                    sql = re_procedure.replaceAll(PROCEDURE);
                    if (sql.indexOf(PROCEDURE) > -1) {
                        String procedure = sql.substring(PROCEDURE.length(), sql.indexOf(LEFTPARENTHESIS));
                        setTableNameHashSet(procedure, tableSet);
                    } else {
                        throw new Exception("sql format is error: sql=" + sql);
                    }
                }
            }
        }
        return tableSet;
    }

    private void setTableNameHashSet(String tablename, HashSet<String> s) {
        if (tablename != null && !"".equals(tablename.trim())) {
            String as[] = tablename.trim().split("\\s");
            tablename = as[0];
            if (tablename.indexOf(".") > 0) {
                tablename = tablename.substring(tablename.indexOf(".") + 1);
            }
            s.add(tablename);
        }
    }

    /**
     * 根据StatementName，解析出表名字
     *
     * @param configuration
     * @param statementName
     * @param parameterObject
     * @param sqlTableCacheEnable
     * @param sqlTableCacheExclusions
     * @return
     */
    public Set<String> getTableNamesByStatementName(Configuration configuration, String statementName, Object parameterObject, boolean sqlTableCacheEnable,
                                                    List<String> sqlTableCacheExclusions) {
        Set<String> result = null;
        if (sqlTableCacheEnable && sqlTableCacheExclusions != null && sqlTableCacheExclusions.contains(statementName)) {
            sqlTableCacheEnable = false;
        }
        if (sqlTableCacheEnable && sqlCache != null) {
            result = (Set<String>) sqlCache.getContent(statementName);
        }
        if (result == null) {
            boolean isDynamicSql = false;
            MappedStatement stmt = configuration.getMappedStatement(statementName);

            String sql = null;
            try {
                BoundSql boundSql = stmt.getBoundSql(wrapCollection(parameterObject));
                sql = boundSql.getSql();
                result = SQLEngine.getInstance().getTableNameSet(sql);
            } catch (Exception e) {
				/*
				Object innerParameterObject = null;
				if (parameterObject instanceof Collection) {
					innerParameterObject = ((Collection) parameterObject).iterator().next();
				} else if (parameterObject != null && parameterObject.getClass().isArray()) {
					int len = Array.getLength(parameterObject);
					if (len > 0) {
						innerParameterObject = Array.get(parameterObject, 0);
					}
				}
				if (innerParameterObject != null) {
					try {
						BoundSql boundSql = stmt.getBoundSql(innerParameterObject);
						sql = boundSql.getSql();
						result = SQLEngine.getInstance().getTableNameSet(sql);
					} catch (Exception e2) {
						throw new RoutingException("Error while parsing sql of statement:" + statementName + ",sql:" + sql, e);
					}
				} else {
					throw new RoutingException("Error while parsing sql of statement:" + statementName + ",sql:" + sql, e);
				}
				*/
                throw new RoutingException("Error while parsing sql of statement:" + statementName + ",sql:" + sql, e);
            }
            if (sqlTableCacheEnable && sqlCache != null && !isDynamicSql) {
                sqlCache.putContent(statementName, result);
                sqlCache.putContent("SQL:" + statementName, sql);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("statement:" + statementName + ", tables:" + result);
        }
        return result;
    }

    private Object wrapCollection(final Object object) {
        if (object instanceof List) {
            DefaultSqlSession.StrictMap<Object> map = new DefaultSqlSession.StrictMap<>();
            map.put("list", object);
            return map;
        } else if (object != null && object.getClass().isArray()) {
            DefaultSqlSession.StrictMap<Object> map = new DefaultSqlSession.StrictMap<>();
            map.put("array", object);
            return map;
        }
        return object;
    }

    /**
     * @param statementName   == sqlId
     * @param parameterObject
     * @return 完整的sql
     */
    public String getSqlByStatementName(Configuration configuration, String statementName, Object parameterObject) {
        MappedStatement stmt = configuration.getMappedStatement(statementName);
        return stmt.getBoundSql(parameterObject).getSql();
    }
}
