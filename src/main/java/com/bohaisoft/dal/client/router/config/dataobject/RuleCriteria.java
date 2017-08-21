/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.router.config.dataobject;

import org.springframework.beans.factory.InitializingBean;

/**
 * 
 * 
 * @author wuxiang
 * @since 2012-3-12
 */
public class RuleCriteria<T> implements InitializingBean {

	private T lt;//less than
	private T le;//less than and equals
	private T gt;//great than
	private T ge;//great than an equals

	public T getLt() {
		return lt;
	}

	public void setLt(T lt) {
		this.lt = lt;
	}

	public T getLe() {
		return le;
	}

	public void setLe(T le) {
		this.le = le;
	}

	public T getGt() {
		return gt;
	}

	public void setGt(T gt) {
		this.gt = gt;
	}

	public T getGe() {
		return ge;
	}

	public void setGe(T ge) {
		this.ge = ge;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (ge != null && gt != null) {
			throw new IllegalArgumentException("Only one property of 'ge','gt' can be set");
		}
		if (le != null && lt != null) {
			throw new IllegalArgumentException("Only one property of 'le','lt' can be set");
		}
		if(ge == null && gt == null
				&& lt == null && le == null) {
			throw new IllegalArgumentException("At least one property should be set");
		}
	}

}
