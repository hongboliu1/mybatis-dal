/**
 * Copyright(c) 2012 yihaodian. All rights reserved.
 * dal-client
 */

package com.bohaisoft.dal.client.router.config.dataobject;


import com.bohaisoft.dal.client.router.rule.ShardingRule;

import java.util.List;

/**
 * sharding rule for single table,including database sharding rule and table sharding rule
 *
 * @author wuxiang
 * @since 2012-3-9
 */
public class ShardingRuleOfTable {

    private String dbType;

    private List<ShardingRule> tbRuleList;

    private List<ShardingRule> dbRuleList;

    /**
     * @return the dbType
     */
    public String getDbType() {
        return dbType;
    }

    /**
     * @param dbType the dbType to set
     */
    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    /**
     * @return the tbRuleList
     */
    public List<ShardingRule> getTbRuleList() {
        return tbRuleList;
    }

    /**
     * @param tbRuleList the tbRuleList to set
     */
    public void setTbRuleList(List<ShardingRule> tbRuleList) {
        this.tbRuleList = tbRuleList;
    }

    /**
     * @return the dbRuleList
     */
    public List<ShardingRule> getDbRuleList() {
        return dbRuleList;
    }

    /**
     * @param dbRuleList the dbRuleList to set
     */
    public void setDbRuleList(List<ShardingRule> dbRuleList) {
        this.dbRuleList = dbRuleList;
    }


}
