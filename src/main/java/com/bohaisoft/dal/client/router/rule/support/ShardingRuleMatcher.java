/**
 * Copyright(c) 2012 yihaodian. All rights reserved.
 * dal-client
 */

package com.bohaisoft.dal.client.router.rule.support;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.bohaisoft.dal.client.cache.CacheFactory;
import com.bohaisoft.dal.client.cache.ICache;
import com.bohaisoft.dal.client.exception.RoutingException;
import com.bohaisoft.dal.client.router.rule.parser.DefaultFelExpParser;
import com.bohaisoft.dal.client.router.rule.parser.IExpParser;
import com.bohaisoft.dal.client.support.reflect.CglibReflectHelper;
import com.bohaisoft.dal.client.support.reflect.JdkReflectHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 分片规则匹配器
 *
 * @author wuxiang
 * @since 2012-3-14
 */
public class ShardingRuleMatcher {

    private static Log log = LogFactory.getLog(ShardingRuleMatcher.class);

    public final static Object ALL_SHARDS = "";

    private static ICache expCache = CacheFactory.INSTANCE.createCache(100);

    private IExpParser expParser = new DefaultFelExpParser();

    private boolean reflectWithCglib = false;

    private ICache ruleCache = null;

    public boolean isReflectWithCglib() {
        return reflectWithCglib;
    }

    public void setReflectWithCglib(boolean reflectWithCglib) {
        this.reflectWithCglib = reflectWithCglib;
    }

    public IExpParser getExpParser() {
        return expParser;
    }

    public void setExpParser(IExpParser expParser) {
        this.expParser = expParser;
    }

    public ICache getRuleCache() {
        return ruleCache;
    }

    public void setRuleCache(ICache ruleCache) {
        this.ruleCache = ruleCache;
    }

    public Object match(String ruleExp, Object parameterObject) throws RoutingException {
        if (log.isDebugEnabled()) {
            log.debug("Sharding rule expression-" + ruleExp);
            log.debug("Sharding rule fact-" + parameterObject);
        }
        Object result = null;

        Set<String> params = (Set<String>) expCache.getContent(ruleExp);
        if (params == null) {
            params = new HashSet<String>();
            String last = ruleExp;
            int start = -1;

            while ((start = last.indexOf("#")) != -1) {// 解析##包围的变量
                last = last.substring(start + 1);
                int end = last.indexOf("#");
                String param = last.substring(0, end);
                params.add(param);
                last = last.substring(end + 1);
            }
            expCache.putContent(ruleExp, params);
        }
        Map<String, Object> paramValues = new HashMap<String, Object>();
        String ruleStr = ruleExp;
        boolean existingNullValue = false;
        for (String param : params) {
            Object value = paramValues.get(param);
            if (value == null) {
                try {
                    value = getParameterValue(ruleExp, param, parameterObject);
                } catch (Exception e) {
                    throw new RoutingException("Failed to parse rule expression- " + ruleExp + " with parameter object-" + parameterObject, e);
                }
                if (value == null) {
                    // throw new
                    // IllegalArgumentException("Could not get value with parameter-"
                    // + param + " from parameter object-" + parameterObject);
                    existingNullValue = true;
                }
                paramValues.put(param, value);
                ruleStr = ruleStr.replaceAll("#" + param + "#", value == null ? "" : value.toString());
            }
        }
        if (ruleCache != null) {
            result = ruleCache.getContent(ruleStr);
        }
        if (result == null) {
            if (existingNullValue) {
                result = ALL_SHARDS;
                if (ruleCache != null) {
                    ruleCache.putContent(ruleStr, result);
                }
                return result;
            } else {
                try {
                    result = expParser.parse(ruleExp.replaceAll("#", ""), paramValues);
                    if (ruleCache != null) {
                        ruleCache.putContent(ruleStr, result);
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Matched sharding rule result-" + result);
                    }
                    return result;
                } catch (Exception e) {
                    try {
                        result = expParser.parse(ruleStr);
                        if (ruleCache != null) {
                            ruleCache.putContent(ruleStr, result);
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("Matched sharding rule result-" + result);
                        }
                        return result;
                    } catch (Exception e1) {
                        throw new RoutingException("Failed to compile Sharding rule:" + ruleStr + ",fact:" + parameterObject, e);
                    }
                }
            }
        } else {
            return result;
        }
    }

    public Object getParameterValue(String ruleExp, String param, Object parameterObject) throws Exception {
        if (parameterObject instanceof Map) {// for Map
            return ((Map) parameterObject).get(param);
        } else if (parameterObject instanceof Collection) {// for Collection,
            // read field value
            // from the first
            // element of this
            // collection
            Collection parameterList = (Collection) parameterObject;
            Iterator ir = parameterList.iterator();
            Object first = ir.next();
            Object firstValue = readField(first, param, reflectWithCglib);
            if (firstValue == null) {
                throw new IllegalArgumentException("Could not get value with parameter-" + param + " from parameter object-" + parameterObject);
            }
            Object firstResult = null;
            while (ir.hasNext()) {
                if (firstResult == null) {
                    firstResult = match(ruleExp, first);
                }
                Object next = ir.next();
                Object nextResult = match(ruleExp, next);
                if (!firstResult.equals(nextResult)) {
                    throw new IllegalArgumentException("Inconsistent sharding value-" + nextResult + ",the first value- " + firstResult + " with parameter-" + param
                            + " in parameter object Collection-" + parameterObject);
                }
            }
            return firstValue;
        } else if (parameterObject != null && parameterObject.getClass().isArray()) {// for
            // Array,
            // read
            // field value from
            // the first element
            // of this array
            Object first = Array.get(parameterObject, 0);
            Object firstValue = readField(first, param, reflectWithCglib);
            if (firstValue == null) {
                throw new IllegalArgumentException("Could not get value with parameter-" + param + " from parameter object-" + parameterObject);
            }
            Object firstResult = null;
            int len = Array.getLength(parameterObject);
            for (int i = 1; i < len; i++) {
                if (firstResult == null) {
                    firstResult = match(ruleExp, first);
                }
                Object next = Array.get(parameterObject, i);
                Object nextResult = match(ruleExp, next);
                if (!firstResult.equals(nextResult)) {
                    throw new IllegalArgumentException("Inconsistent sharding value-" + nextResult + ",the first value- " + firstResult + " with parameter-" + param
                            + " in parameter object Array-" + parameterObject);
                }
            }
            return firstValue;
        }

        return readField(parameterObject, param, reflectWithCglib);
    }

    public Object readField(Object parameterObject, String fieldName, boolean reflectWithCglib) throws Exception {
        if (reflectWithCglib) {
            return CglibReflectHelper.readField(parameterObject, fieldName);
        } else {
            return JdkReflectHelper.readField(parameterObject, fieldName);
        }
    }
}
