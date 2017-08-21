/**
 * Copyright(c) 2012 yihaodian. All rights reserved.
 * dal-client
 */

package com.bohaisoft.dal.client.support.reflect;

import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 反射工具类
 *
 * @author wuxiang
 * @since 2012-4-1
 */
public class CglibReflectHelper {

    private static ConcurrentHashMap<String, FastMethod> fieldsCache = new ConcurrentHashMap<String, FastMethod>();

    public static Object readField(Object parameterObject, String fieldName) throws Exception {
        if (parameterObject == null) {
            throw new IllegalArgumentException("target object must not be null");
        }
        Class cls = parameterObject.getClass();
        String key = parameterObject.getClass().getCanonicalName() + "#" + fieldName;

        FastMethod prop = fieldsCache.get(key);// 从字段缓存里查找
        if (prop == null) {
            synchronized (fieldsCache) {
                prop = fieldsCache.get(key);
                if (prop == null) {
                    String methodName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
                    final FastClass fastClass = FastClass.create(cls);
                    Class[] parameterTypes = new Class[0];
                    prop = fastClass.getMethod(methodName, parameterTypes);
                    if (prop == null) {
                        throw new IllegalArgumentException("Cannot locate field " + fieldName + " on " + cls);
                    }
                    fieldsCache.putIfAbsent(key, prop);
                }
            }
        }
        Object value = null;
        try {
            value = prop.invoke(parameterObject, new Object[]{});// 调用get方法
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }

        return value;
    }
}
