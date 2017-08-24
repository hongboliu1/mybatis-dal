package com.bohaisoft.dal.util;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

/**
 * Created by liuhb on 2016/12/30.
 */
public class PrototypeLoadBalanceDataSource extends AbstractRoutingDataSource {

    @Override
    protected Object determineCurrentLookupKey() {
        return DBContextHolder.getJdbcType();
    }

}
