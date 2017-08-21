/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router.rule.parser;

import java.util.Map;

/**
 * 
 * 
 * @author wuxiang
 * @since 2012-5-25
 */
public interface IExpParser {

	public Object parse(String exp) throws Exception;
	
	public Object parse(String exp, Map<String, Object> params) throws Exception;
}
