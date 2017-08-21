package com.bohaisoft.dal.client.datasource;

import com.bohaisoft.dal.client.datasource.ha.FailoverDS;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;

public class SlaveDS extends FailoverDS implements InitializingBean {

    private String name;
    private int weight;
    private AtomDS dataSource;
    private AtomDS slaveDetector;

    /**
     * @return the slaveDetector
     */
    public AtomDS getSlaveDetector() {
        return slaveDetector;
    }

    /**
     * @param slaveDetector the slaveDetector to set
     */
    public void setSlaveDetector(AtomDS slaveDetector) {
        this.slaveDetector = slaveDetector;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * 权重
     */
    public int getWeight() {
        return weight;
    }

    /**
     * 权重
     */
    public void setWeight(int weight) {
        this.weight = weight;
    }

    public AtomDS getDataSource() {
        return dataSource;
    }

    public void setDataSource(AtomDS dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void afterPropertiesSet() {
        if (StringUtils.isEmpty(this.name)) {
            throw new IllegalArgumentException("the 'name' property of SlaveDS must be set");
        }
        this.dataSource.setParentDS(this);
    }

    @Override
    public String toString() {
        return this.getName();
    }
}
