/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router.rule.hash;

/**
 * hash函数接口
 * 
 * @author wuxiang
 * @since 2012-3-21
 */
public interface HashFunction {

	public Object hash(Object key);
}
