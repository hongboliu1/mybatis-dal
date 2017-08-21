/**
 * Copyright(c) 2012 yihaodian. All rights reserved.
 * dal-client-1.1.0
 */
package com.bohaisoft.dal.client.concurrent;


import com.bohaisoft.dal.client.router.config.dataobject.ShardingMapping;
import com.bohaisoft.dal.client.router.support.RouterFactCtx;
import com.bohaisoft.dal.client.router.support.RouterFactCtxVO;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 实现SqlMapClientCallback接口的抽象回调类，用于动态获取参数
 *
 * @author wuxiang
 * @since 2013-1-7
 */
public abstract class DefaultSqlMapClientCallback implements SqlMapClientCallback {

    private Object initialParameterObject = null;

    public Object getInitialParameterObject() {
        return initialParameterObject;
    }

    public void setInitialParameterObject(Object initialParameterObject) {
        this.initialParameterObject = initialParameterObject;
    }

    /**
     * 动态获取参数对象
     * @return
     */
    public Object getParameterObject() {
        RouterFactCtxVO fact = RouterFactCtx.getRfvoholder();
        Object parameters = null;
        if (fact != null && fact.getParameters() != null) {
            parameters = fact.getParameters();
        } else {
            parameters = initialParameterObject;
        }
        if (fact != null && parameters instanceof Collection) {
            List<ShardingMapping> mappings = fact.getVirtualTablesMapping();
            if (mappings != null && mappings.size() > 1) {
                List<Object> newParameters = new ArrayList<>();
                for (ShardingMapping mapping : mappings) {
                    newParameters.addAll(mapping.getParameterObjects());
                }
                return newParameters;
            }
        }
        return parameters;
    }

}
