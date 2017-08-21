package com.bohaisoft.dal.util;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/**
 * Created by ThinkPad on 2017/1/1.
 */
@Aspect
@Component
public class DataSourceAspect {

    private final static Logger LOGGER = LoggerFactory.getLogger(DataSourceAspect.class);

    @Pointcut("execution(* com.bohaisoft.dal.mapper.*.*(..))")
    public void selectDataSource() {

    }

    @Before("selectDataSource()")
    public void before(JoinPoint point) {
        DBContextHolder.clearJdbcType();
        Object target = point.getTarget();
        String methodName = point.getSignature().getName();
        Class<?>[] classes = target.getClass().getInterfaces();
        Class<?>[] parameterTypes = ((MethodSignature) point.getSignature()).getMethod().getParameterTypes();

        try {
            Method method = classes[0].getMethod(methodName, parameterTypes);
            if (method != null && (method.isAnnotationPresent(DataSource.class))) {
                DataSource dataSource = method.getAnnotation(DataSource.class);
                if (dataSource != null) {
                    DBContextHolder.setJdbcType(dataSource.value());
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("datasource value is : " + dataSource.value());
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
