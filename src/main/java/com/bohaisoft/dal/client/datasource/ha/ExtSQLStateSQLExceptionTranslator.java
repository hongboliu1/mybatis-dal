/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.datasource.ha;

import java.sql.SQLException;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.support.SQLStateSQLExceptionTranslator;

/**
 *
 *	
 * @author wuxiang
 * @since 2012-4-19
 */
public class ExtSQLStateSQLExceptionTranslator extends SQLStateSQLExceptionTranslator {

	protected DataAccessException doTranslate(String task, String sql, SQLException ex) {
		String sqlState = ex.getSQLState();
		if (sqlState == null) {
			SQLException nestedEx = ex.getNextException();
			if (nestedEx != null) {
				sqlState = nestedEx.getSQLState();
			} else if(ex.getCause() != null && (ex.getCause() instanceof SQLException)){
				nestedEx = (SQLException)ex.getCause();
				if (nestedEx != null) {
					ex.setNextException(nestedEx);
					sqlState = nestedEx.getSQLState();
				} 
			}
		}
		if(sqlState == null && ex.getMessage() != null) {
			if(ex.getMessage().indexOf("The Network Adapter could not establish the connection") != -1) {
				return new DataAccessResourceFailureException(buildMessage(task, sql, ex), ex);
			}
		}
		return super.doTranslate(task, sql, ex);
	}
	
}
