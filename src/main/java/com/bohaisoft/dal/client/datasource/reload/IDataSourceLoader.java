/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.datasource.reload;

import javax.sql.DataSource;


/**
*	
* @author wuxiang
* @since 2012-6-11
*/
public interface IDataSourceLoader {

	/**
	 * reload data source
	 */
	public void reload(DataSource dataSource);
	
}
