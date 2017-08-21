package com.bohaisoft.dal.client.datasource;

import com.bohaisoft.dal.client.datasource.ha.FailoverDS;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.Advised;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

import javax.sql.DataSource;

public class AtomDS implements DisposableBean, InitializingBean {

    static final Logger logger = LoggerFactory.getLogger(AtomDS.class);
    //private static Set<String> idSet = new HashSet<String>();

    private String id;
    private boolean isNeedTransaction = false;
    private boolean isReadOnly = false;// 是否只读数据源
    private String description;
    private DataSource targetDataSource;
    private int coreSize = Runtime.getRuntime().availableProcessors();// 连接池coreSize
    private int poolSize = Runtime.getRuntime().availableProcessors() * 2;// 连接池poolSize
    private int maxRequest = 500;// 最大请求数
    private boolean isEnabled = true;
    private boolean isLazyConnect = false;
    private GroupDS groupDS;//所属数据源组
    private FailoverDS parentDS;//所属上级数据源
    private DataSource originalDataSource;
    private String targetId;

    public FailoverDS getParentDS() {
        return parentDS;
    }

    public void setParentDS(FailoverDS parentDS) {
        this.parentDS = parentDS;
    }

    public DataSource getOriginalDataSource() {
        return originalDataSource;
    }

    public void setOriginalDataSource(DataSource originalDataSource) {
        this.originalDataSource = originalDataSource;
    }

    public GroupDS getGroupDS() {
        return groupDS;
    }

    public void setGroupDS(GroupDS groupDS) {
        this.groupDS = groupDS;
    }

    public boolean isEnabled() {
        return isEnabled;
    }

    public void setEnabled(boolean isEnabled) {
        this.isEnabled = isEnabled;
    }

    /**
     * 目标数据源
     */
    public DataSource getTargetDataSource() {
        return targetDataSource;
    }

    /**
     * 目标数据源
     */
    public void setTargetDataSource(DataSource targetDataSource) {
        if (isLazyConnect) {
            this.targetDataSource = new LazyConnectionDataSourceProxy(targetDataSource);
        } else {
            this.targetDataSource = targetDataSource;
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public void setReadOnly(boolean isReadOnly) {
        this.isReadOnly = isReadOnly;
    }

    public boolean isNeedTransaction() {
        return isNeedTransaction;
    }

    public void setIsNeedTransaction(boolean isNeedTransaction) {
        this.isNeedTransaction = isNeedTransaction;
    }

    public int getCoreSize() {
        return coreSize;
    }

    public void setCoreSize(int coreSize) {
        this.coreSize = coreSize;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public int getMaxRequest() {
        return maxRequest;
    }

    public void setMaxRequest(int maxRequest) {
        this.maxRequest = maxRequest;
    }

    @Override
    public void afterPropertiesSet() {
        if (StringUtils.isEmpty(this.id)) {
            throw new IllegalArgumentException("The property 'id' is required");
        }
        targetId = getTargetIdString();
        setOriginalDataSource(getTargetDataSource());
        checkPoolSize();
    }

    public void checkPoolSize() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        if (coreSize <= 0) {
            coreSize = availableProcessors;
        }
        if (poolSize > 0 && poolSize < coreSize) {
            coreSize = poolSize;
        }
        if (poolSize <= 0) {
            poolSize = coreSize * 2;
        } else if (poolSize > 0 && poolSize > coreSize * 8) {
            poolSize = coreSize * 8;
        }
    }

    @Override
    public String toString() {
        return this.getId() + (this.getDescription() != null ? ("-" + this.getDescription()) : "");
    }

    public void destroy() throws Exception {
    }

    /**
     * 真实数据源id
     *
     * @return
     */
    public String getTargetId() {
        if (parentDS.isPassiveFailoverEnable() || parentDS.isPositiveFailoverEnable()) {
            return getTargetIdString();
        } else if (targetId == null) {
            targetId = getTargetIdString();
            return targetId;
        } else {
            return targetId;
        }
    }

    private String getTargetIdString() {
        String dsId = id;
        DataSource ds = getTargetDataSource();
        if (ds != null) {
            dsId = dsId + ":" + ds.toString();
            if (ds instanceof Advised) {
                TargetSource targetSource = ((Advised) ds).getTargetSource();
                if (targetSource != null) {
                    try {
                        dsId = dsId + targetSource.getTarget().hashCode();
                    } catch (Exception e) {
                    }
                }
            }
        }
        return dsId;
    }

}
