/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */
package com.bohaisoft.dal.client.router.rule.parser;

import java.util.Map;

import com.bohaisoft.dal.client.cache.CacheFactory;
import com.bohaisoft.dal.client.cache.ICache;
import com.greenpineyu.fel.Expression;
import com.greenpineyu.fel.FelEngine;
import com.greenpineyu.fel.FelEngineImpl;
import com.greenpineyu.fel.context.FelContext;
import com.greenpineyu.fel.context.MapContext;
import org.springframework.util.CollectionUtils;


/**
 * 
 * 
 * @author wuxiang
 * @since 2012-12-5
 */
public abstract class AbstractFelExpParser implements IExpParser {

	private FelEngine fel = new FelEngineImpl();
	private static ICache felCache = CacheFactory.INSTANCE.createCache(100);

	public Object parse(String exp) throws Exception {
		return parse(exp, null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.yihaodian.ydal.client.router.rule.parser.IExpParser#parse(java.lang
	 * .String, java.util.Map)
	 */
	@Override
	public Object parse(String exp, Map<String, Object> params) throws Exception {
		Map<String, Object> addedParams = addParameters();
		if (!CollectionUtils.isEmpty(addedParams)) {
			if(params != null) {
				params.putAll(addedParams);
			} else {
				params = addedParams;
			}
		}
		FelContext ctxt = new MapContext(params);
		Expression expression = null;
		try {
			expression = (Expression) felCache.getContent(exp);
		}catch(Exception e) {
			e.printStackTrace();
		}
		if (expression == null) {
			expression = fel.compile(exp, ctxt);
			felCache.putContent(exp, expression);
		}
		return expression.eval(ctxt);
	}

	public abstract Map<String, Object> addParameters();
}
