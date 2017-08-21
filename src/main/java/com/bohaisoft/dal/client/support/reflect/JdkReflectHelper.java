/**
 *  Copyright(c) 2012 yihaodian. All rights reserved.
 *  dal-client
 */

package com.bohaisoft.dal.client.support.reflect;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 反射工具类
 * 
 * @author wuxiang
 * @since 2012-4-1
 */
public class JdkReflectHelper {

	private static ConcurrentHashMap<String, AccessibleObject> fieldsCache = new ConcurrentHashMap<String, AccessibleObject>();
	
	public static Object readField(Object parameterObject, String fieldName) throws Exception {
		if (parameterObject == null) {
			throw new IllegalArgumentException("target object must not be null");
		}
		Class cls = parameterObject.getClass();
		String key = parameterObject.getClass().getCanonicalName() + "#" + fieldName;
		
		AccessibleObject prop = fieldsCache.get(key);//从字段缓存里查找
		if (prop == null) {
			synchronized(fieldsCache) {
				prop = fieldsCache.get(key);
				if (prop == null) {
					prop = FieldUtils.getField(cls, fieldName, true);
					if (prop == null) {//读取get方法
						String methodName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
						Class[] parameterTypes = new Class[0];
						prop = MethodUtils.getMatchingAccessibleMethod(parameterObject.getClass(), methodName, parameterTypes);
						if (prop == null) {
							throw new IllegalArgumentException("Cannot locate field " + fieldName + " on " + cls);
						}
					}
					prop.setAccessible(true);
					fieldsCache.putIfAbsent(key, prop);
				}
			}
		}
		Object value = null;
		try {
			if (prop instanceof Field) {
				Field field = (Field) prop;
				value = FieldUtils.readField(field, parameterObject, true);
			} else if (prop instanceof Method) {
				Method method = (Method) prop;
				value = method.invoke(parameterObject, ArrayUtils.EMPTY_OBJECT_ARRAY);
			}
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}

		return value;
	}
}
